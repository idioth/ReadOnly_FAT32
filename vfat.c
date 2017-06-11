// vim: noet:ts=4:sts=4:sw=4:et
#define FUSE_USE_VERSION 26
#define _GNU_SOURCE

#include <assert.h>
#include <endian.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <iconv.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "vfat.h"
#include "util.h"
#include "debugfs.h"

#define DEBUG_PRINT(...) printf(__VA_ARGS)
#define MAX_NAME_SIZE (13 * 0x14)

iconv_t iconv_utf16;
char* DEBUGFS_PATH = "/.debug";


static void
vfat_init(const char *dev)
{
    struct fat_boot_header s;

    iconv_utf16 = iconv_open("utf-8", "utf-16"); // from utf-16 to utf-8
    // These are useful so that we can setup correct permissions in the mounted directories
    vfat_info.mount_uid = getuid();
    vfat_info.mount_gid = getgid();

    // Use mount time as mtime and ctime for the filesystem root entry (e.g. "/")
    vfat_info.mount_time = time(NULL);

    vfat_info.fd = open(dev, O_RDONLY);
    if (vfat_info.fd < 0)
        err(1, "open(%s)", dev);
    if (pread(vfat_info.fd, &s, sizeof(s), 0) != sizeof(s))
        err(1, "read super block");

    /* XXX add your code here */
    /* vfat_data from boot sector */
    vfat_info.bytes_per_cluster = s.bytes_per_sector * s.sectors_per_cluster;
    vfat_info.bytes_per_sector = s.bytes_per_sector;
    vfat_info.sectors_per_cluster = s.sectors_per_cluster;
    vfat_info.reserved_sectors = s.reserved_sectors;
    vfat_info.sectors_per_fat = s.sectors_per_fat;
    vfat_info.fat_entries = s.sectors_per_fat / s.sectors_per_cluster;
    vfat_info.fat_size = (s.sectors_per_fat_small != 0) ? s.sectors_per_fat_small : s.sectors_per_fat;
    vfat_info.total_sector = (s.total_sectors_small != 0) ? s.total_sectors_small : s.total_sectors;
    vfat_info.count_of_cluster = (vfat_info.total_sector - (vfat_info.reserved_sectors + ( vfat_info.fat_size * s.fat_count))) / vfat_info.sectors_per_cluster;
    vfat_info.cluster_begin_offset = (vfat_info.reserved_sectors + 2 * vfat_info.fat_size) * vfat_info.bytes_per_sector;

    /* fat 12 */
    if(vfat_info.count_of_cluster < 4085)
        return;
    /* fat 16 */
    else if(vfat_info.count_of_cluster < 65525)
        return;
    /* fat 32 */
    else if(!((s.fat_flags >> 7) & 1))
        vfat_info.active_fat = (s.fat_flags & 1);

    /* mapping the fat32 */
    vfat_info.fat_begin_offset = (vfat_info.reserved_sectors + vfat_info.active_fat * vfat_info.sectors_per_fat) * vfat_info.bytes_per_sector;
    vfat_info.fat = mmap_file(vfat_info.fd, vfat_info.fat_begin_offset, vfat_info.fat_size);
    /* XXX ENd */

    vfat_info.root_inode.st_ino = le32toh(s.root_cluster);
    vfat_info.root_inode.st_mode = 0555 | S_IFDIR;
    vfat_info.root_inode.st_nlink = 1;
    vfat_info.root_inode.st_uid = vfat_info.mount_uid;
    vfat_info.root_inode.st_gid = vfat_info.mount_gid;
    vfat_info.root_inode.st_size = 0;
    vfat_info.root_inode.st_atime = vfat_info.root_inode.st_mtime = vfat_info.root_inode.st_ctime = vfat_info.mount_time;

}

/* XXX add your code here */
/* checksum for long file name */
unsigned char
chkSum (unsigned char *pFcbName) {
    short fcbNameLen;
    unsigned char sum;

    sum = 0;
    for (fcbNameLen=11; fcbNameLen!=0; fcbNameLen--) {
        // NOTE: The operation is an unsigned char rotate right
        sum = ((sum & 1) ? 0x80 : 0) + (sum >> 1) + *pFcbName++;
    }
    return (sum);
}

int vfat_next_cluster(uint32_t c)
{
    /* find next cluster */
    /* TODO: Read FAT to actually get the next cluster */
    return vfat_info.fat[c]; // no next cluster
}

int vfat_readdir(uint32_t first_cluster, fuse_fill_dir_t callback, void *callbackdata)
{
    struct stat st; // we can reuse same stat entry over and over again
    int i, seq = 0;
    uint8_t csum = '\0';
    char *longname = calloc(MAX_NAME_SIZE, sizeof(char));

    /* vfat file's id */
    st.st_uid = vfat_info.mount_uid;
    st.st_gid = vfat_info.mount_gid;
    st.st_nlink = 1;

    /* XXX add your code here */
    struct fat32_direntry direntry;
    struct fat32_direntry_long direntry_long;
    int count = 0; char direcory_end = 1; long current_cluster = first_cluster;

    while(current_cluster < 268435448 && direcory_end != 0)
    {
        count = 0; // number of directories in current cluster
        do
        {
            long place = vfat_info.cluster_begin_offset + (current_cluster - 2) * vfat_info.sectors_per_cluster * vfat_info.bytes_per_sector + 32 * count;

            if(pread(vfat_info.fd, &direntry, sizeof(direntry), place) != sizeof(direntry))
                err(1, "read direntry 2");

            else
            {
                if((direntry.nameext[0] & 0xFF) != 0xE5) // not deleted file
                {
                    if(direntry.attr == VFAT_ATTR_LFN) // if it is lfn
                    {
                        direntry_long = *((struct fat32_direntry_long *)&direntry); // short_entry => long_entry
                        csum = direntry_long.csum; // save checksum for short_entry

                        if((direntry_long.seq & VFAT_LFN_SEQ_START) == VFAT_LFN_SEQ_START) // End of LFN
                        {

                            seq = (direntry_long.seq & VFAT_LFN_SEQ_MASK) -1; // ex) 0x41 & VFAT_LFN_SEQ_MASK = 1

                            for(i = 0; i < 13; i++)
                            {
                                if(i < 5 && direntry_long.name1[i] != 0xFF) // name1
                                {
                                    longname[i] = direntry_long.name1[i];
                                }

                                else if(i < 11 && direntry_long.name2[i] != 0xFF) // name2
                                {
                                    longname[i] = direntry_long.name2[i - 5];
                                }

                                else if(i < 13 && direntry_long.name3[i] != 0xFF) // name3
                                {
                                    longname[i] = direntry_long.name3[i - 11];
                                }
                            }
                        }

                        else if(csum == direntry_long.csum && direntry_long.seq == seq) // seq = 1, 2, 3... not 0x40~~
                        {
                            char tmp[MAX_NAME_SIZE];    // add for End of LFN's longname
                            memset(tmp, 0, MAX_NAME_SIZE);

                            for(i = 0; i < MAX_NAME_SIZE; i++)  // save file name
                                tmp[i] = longname[i];

                            memset(longname, 0, MAX_NAME_SIZE); // init

                            seq -= 1;   // order - 1

                            for(i = 0; i < MAX_NAME_SIZE; i++)
                            {
                                if(i < 5 && direntry_long.name1[i] != 0xFF) // name1
                                {
                                    longname[i] = direntry_long.name1[i];
                                }

                                else if(i < 11 && direntry_long.name2[i] != 0xFF)   // name2
                                {
                                    longname[i] = direntry_long.name2[i - 5];
                                }

                                else if(i < 13 && direntry_long.name3[i] != 0xFF)   // name3
                                {
                                    longname[i] = direntry_long.name3[i - 11];
                                }

                                else if(i >= 13 && tmp[i-13] != 0xFF)   // add before LFN name
                                {
                                    longname[i] = tmp[(i-13)];
                                }
                            }
                        }

                        else    // Invalid order or checksum
                        {
                            seq = 0;
                            csum = '\0';
                            memset(longname, 0, MAX_NAME_SIZE);
                            err(1, "Invalid sequence or chekcsum\n");
                        }
                    }
                    else if(direntry.attr == 0x08)  // Volume Label
                    {
                        seq = 0;
                        csum = '\0';
                        memset(longname, 0, MAX_NAME_SIZE);
                    }

                    else if(csum == chkSum((unsigned char *)&(direntry.nameext)) && seq == 0)   // Short_entry what has long file name
                    {
                        st.st_mode = S_IRWXU | S_IRWXG | S_IRWXO;

                        st.st_mode |= ((direntry.attr >> 4) & 1) ? S_IFDIR : S_IFREG;

                        /* size */
                        st.st_size = direntry.size;
                        fflush(stdout);

                        struct tm info;

                        /* last accessed data */
                        info.tm_year = (((direntry.atime_date >> 9) & 0x3F) + 80);
                        info.tm_mon = ((direntry.atime_date >> 5) & 0xF) - 1;
                        info.tm_mday = (direntry.atime_date & 0x1F);

                        st.st_atime = (direntry.atime_date > 0) ? mktime(&info) : 0;

                        /* last modified time */
                        info.tm_year = (((direntry.mtime_date >> 9) & 0x3F) + 80);
                        info.tm_mon = ((direntry.mtime_date >> 5) & 0xF) - 1;
                        info.tm_mday = (direntry.mtime_date & 0x1F);

                        info.tm_hour = ((direntry.mtime_time >> 11) & 0x1F) + 1;
                        info.tm_min = ((direntry.mtime_time >> 5) & 0x3F);
                        info.tm_sec = 2 * (direntry.mtime_time & 0x1F);

                        st.st_mtime = (direntry.mtime_date > 0) ? mktime(&info) : 0;

                        /* created time */
                        info.tm_year = (((direntry.ctime_date >> 9) & 0x3F) + 80);
                        info.tm_mon = ((direntry.ctime_date >> 5) & 0xF) - 1;
                        info.tm_mday = (direntry.ctime_date & 0x1F);

                        info.tm_hour = ((direntry.ctime_time >> 11) & 0x1F) + 1;
                        info.tm_min = ((direntry.ctime_time >> 5) & 0x3F);
                        info.tm_sec = 2 * (direntry.ctime_time & 0x1F);

                        st.st_ctime = (direntry.ctime_date > 0) ? mktime(&info) : 0;

                        st.st_dev = 0;
                        st.st_blocks = 2;
                        st.st_blksize = 4;
                        st.st_ino = direntry.cluster_hi * 256 * 256 + direntry.cluster_lo;

                        /* callback file name */
                        callback(callbackdata, longname, &st, 0);

                        /* init for next directory */
                        csum = '\0';
                        memset(longname, 0, MAX_NAME_SIZE);
                    }

                    else
                    {
                        st.st_mode = S_IRWXU | S_IRWXG | S_IRWXO;

                        st.st_mode |= ((direntry.attr >> 4) & 1) ? S_IFDIR : S_IFREG;

                        /* size */
                        st.st_size = direntry.size;
                        fflush(stdout);

                        struct tm info;

                        /* last accessed time */
                        info.tm_year = (((direntry.atime_date >> 9) & 0x3F) + 80);
                        info.tm_mon = ((direntry.atime_date >> 5) & 0xF) - 1;
                        info.tm_mday = (direntry.atime_date & 0x1F);

                        st.st_atime = (direntry.atime_date > 0) ? mktime(&info) : 0;

                        /* last modified time */
                        info.tm_year = (((direntry.mtime_date >> 9) & 0x3F) + 80);
                        info.tm_mon = ((direntry.mtime_date >> 5) & 0xF) - 1;
                        info.tm_mday = (direntry.mtime_date & 0x1F);

                        info.tm_hour = ((direntry.mtime_time >> 11) & 0x1F) + 1;
                        info.tm_min = ((direntry.mtime_time >> 5) & 0x3F);
                        info.tm_sec = 2 * (direntry.mtime_time & 0x1F);

                        st.st_mtime = (direntry.mtime_date > 0) ? mktime(&info) : 0;

                        /* Created time */
                        info.tm_year = (((direntry.ctime_date >> 9) & 0x3F) + 80);
                        info.tm_mon = ((direntry.ctime_date >> 5) & 0xF) - 1;
                        info.tm_mday = (direntry.ctime_date & 0x1F);

                        info.tm_hour = ((direntry.ctime_time >> 11) & 0x1F) + 1;
                        info.tm_min = ((direntry.ctime_time >> 5) & 0x3F);
                        info.tm_sec = 2 * (direntry.ctime_time & 0x1F);

                        st.st_ctime = (direntry.ctime_date > 0) ? mktime(&info) : 0;

                        st.st_dev = 0;
                        st.st_blocks = 2;
                        st.st_blksize = 4;
                        st.st_ino = direntry.cluster_hi * 256 * 256 + direntry.cluster_lo;

                        /* short file name */
                        char newname[13] = "ZZZZZZZZZZZ";
                        char ext_exist = 0; char padding_end = 0;

                        int index = 11;
                        while(index > 8)
                        {
                            if(direntry.nameext[(index-1)] != '\ ')
                            {
                                ext_exist = 1;
                                newname[index] = direntry.nameext[(index - 1)];
                            }
                            else
                                newname[index] = 0;
                            index--;
                        }

                        if(ext_exist == 1)
                        {
                            newname[index] = '.'; index --;
                        }

                        while(index > -1)
                        {
                            if(direntry.nameext[index] == '\ ' && padding_end == 0)
                            {
                                newname[index] = newname[index+1];
                                newname[index+1] = newname[index+2];
                                newname[index+2] = newname[index+3];
                                newname[index+3] = newname[index+4];
                                newname[index+4] = 0;
                            }

                            else
                            {
                                padding_end = 1;
                                newname[index] = direntry.nameext[index];
                            }
                            index--;
                        }
                            
                        if(callback(callbackdata, newname, &st, 0)){
                        }

                    }
                    
                }

            } count++;

            pread(vfat_info.fd, &direcory_end, sizeof(direcory_end), place);
        } while(direcory_end != 0 && count < (vfat_info.bytes_per_cluster / (32 * 4)));
        /* find next cluster */
        current_cluster = vfat_next_cluster(current_cluster);
    }
    /* free long file name buffer */
    free(longname);
    fflush(stdout);
    /* XXX End */
    return 0;
}


// Used by vfat_search_entry()
struct vfat_search_data {
    const char*  name;
    int          found;
    struct stat* st;
};


// You can use this in vfat_resolve as a callback function for vfat_readdir
// This way you can get the struct stat of the subdirectory/file.
int vfat_search_entry(void *data, const char *name, const struct stat *st, off_t offs)
{
    struct vfat_search_data *sd = data;

    if (strcmp(sd->name, name) != 0) return 0;

    sd->found = 1;
    *sd->st = *st;

    return 1;
}

/**
 * Fills in stat info for a file/directory given the path
 * @path full path to a file, directories separated by slash
 * @st file stat structure
 * @returns 0 iff operation completed succesfully -errno on error
*/
int vfat_resolve(const char *path, struct stat *st)
{
    /* TODO: Add your code here.
        You should tokenize the path (by slash separator) and then
        for each token search the directory for the file/dir with that name.
        You may find it useful to use following functions:
        - strtok to tokenize by slash. See manpage
        - vfat_readdir in conjuction with vfat_search_entry
    */
    int res = -ENOENT; // Not Found
    int debug = 0;
    long cluster = vfat_info.root_inode.st_ino;

    if (strcmp("/", path) == 0)\
    {
        *st = vfat_info.root_inode;
        res = 0;
    }
    else
    {
        struct vfat_search_data sd;
        struct stat st_test; sd.st = &st_test;

        sd.name = strtok((char *) path, "/");

        while(sd.name != NULL)
        {
            sd.found = 0;
            vfat_readdir(cluster, vfat_search_entry, &sd);
            if(sd.found == 1)
            {
                cluster = sd.st->st_ino;
                *st = *sd.st;
                sd.name = strtok(NULL, "/");
                res = 0; debug++;
            }
            else
                break;
        }
    }
    return res;
}

// Get file attributes
int vfat_fuse_getattr(const char *path, struct stat *st)
{
    if (strncmp(path, DEBUGFS_PATH, strlen(DEBUGFS_PATH)) == 0) {
        // This is handled by debug virtual filesystem
        return debugfs_fuse_getattr(path + strlen(DEBUGFS_PATH), st);
    } else {
        // Normal file
        return vfat_resolve(path, st);
    }
}

// Extended attributes useful for debugging
int vfat_fuse_getxattr(const char *path, const char* name, char* buf, size_t size)
{
    struct stat st;
    int ret = vfat_resolve(path, &st);
    if (ret != 0) return ret;
    if (strcmp(name, "debug.cluster") != 0) return -ENODATA;

    if (buf == NULL) {
        ret = snprintf(NULL, 0, "%u", (unsigned int) st.st_ino);
        if (ret < 0) err(1, "WTF?");
        return ret + 1;
    } else {
        ret = snprintf(buf, size, "%u", (unsigned int) st.st_ino);
        if (ret >= size) return -ERANGE;
        return ret;
    }
}

int vfat_fuse_readdir(
        const char *path, void *callback_data,
        fuse_fill_dir_t callback, off_t unused_offs, struct fuse_file_info *unused_fi)
{
    if (strncmp(path, DEBUGFS_PATH, strlen(DEBUGFS_PATH)) == 0) {
        // This is handled by debug virtual filesystem
        return debugfs_fuse_readdir(path + strlen(DEBUGFS_PATH), callback_data, callback, unused_offs, unused_fi);
    }{
        struct stat st;
        vfat_resolve(path, &st);
        vfat_readdir(st.st_ino, callback, callback_data);
    }
    /* TODO: Add your code here. You should reuse vfat_readdir and vfat_resolve functions
    */
    return 0;
}

int vfat_fuse_read(
        const char *path, char *buf, size_t size, off_t offs,
        struct fuse_file_info *unused)
{
    if (strncmp(path, DEBUGFS_PATH, strlen(DEBUGFS_PATH)) == 0) {
        // This is handled by debug virtual filesystem
        return debugfs_fuse_read(path + strlen(DEBUGFS_PATH), buf, size, offs, unused);
    }
    /* TODO: Add your code here. Look at debugfs_fuse_read for example interaction.
    */

    struct stat st; int current = 0; int read_byte = vfat_info.bytes_per_cluster;

    vfat_resolve(path, &st);

    char tmpbuf[size];
    int len = (int)st.st_size - offs;

    if(len <= 0) return 0;
    
    if(len > size)
        len = size;

    int where_is = offs; int current_cluster = st.st_ino;

    while(where_is > vfat_info.bytes_per_cluster)
    {
        current_cluster = vfat_next_cluster(current_cluster);
        where_is -= vfat_info.bytes_per_cluster;
    } current += where_is;

    long place = 0;
    while(current < len)
    {
        place = vfat_info.cluster_begin_offset + (current_cluster - 2) * vfat_info.sectors_per_cluster * vfat_info.bytes_per_sector;

        if(pread(vfat_info.fd, tmpbuf, read_byte, place) != read_byte)
            err(1, "read super block");

        memcpy(buf + current, tmpbuf, read_byte);

        if((len - current) > vfat_info.bytes_per_cluster)
        {
            current += vfat_info.bytes_per_cluster;
            current_cluster = vfat_next_cluster(current_cluster);
        }
        else
        {
            current += (len - current);
            read_byte = (len - current);
        }
    }
    return 0;
}

////////////// No need to modify anything below this point
int
vfat_opt_args(void *data, const char *arg, int key, struct fuse_args *oargs)
{
    if (key == FUSE_OPT_KEY_NONOPT && !vfat_info.dev) {
        vfat_info.dev = strdup(arg);
        return (0);
    }
    return (1);
}

struct fuse_operations vfat_available_ops = {
    .getattr = vfat_fuse_getattr,
    .getxattr = vfat_fuse_getxattr,
    .readdir = vfat_fuse_readdir,
    .read = vfat_fuse_read,
};

int main(int argc, char **argv)
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    fuse_opt_parse(&args, NULL, NULL, vfat_opt_args);

    if (!vfat_info.dev)
        errx(1, "missing file system parameter");

    vfat_init(vfat_info.dev);
    return (fuse_main(args.argc, args.argv, &vfat_available_ops, NULL));
}
