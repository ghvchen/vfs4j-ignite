package org.dcache.vfs4j;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import javax.cache.CacheException;
import javax.security.auth.Subject;

import jnr.ffi.provider.FFIProvider;
import jnr.constants.platform.Errno;
import jnr.ffi.Address;
import jnr.ffi.annotations.*;
import org.dcache.nfs.status.*;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.dcache.auth.Subjects;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.SimpleIdMap;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.vfs.AclCheckable;
import org.dcache.nfs.vfs.DirectoryEntry;
import org.dcache.nfs.vfs.DirectoryStream;
import org.dcache.nfs.vfs.FsStat;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A file system which implementation with is backed-up with a local file
 * system.
 */
public class LocalVFS implements VirtualFileSystem {

    private final Logger LOG = LoggerFactory.getLogger(LocalVFS.class);

    // stolen from /usr/include/bits/fcntl-linux.h
    private final static int O_DIRECTORY = 0200000;
    private final static int O_RDONLY = 00;
    private final static int O_WRONLY = 01;
    private final static int O_RDWR = 02;

    private final static int O_PATH = 010000000;
    private final static int O_NOFOLLOW	= 0400000;
    private final static int O_EXCL = 0200;
    private final static int O_CREAT = 0100;

    private final static int AT_SYMLINK_NOFOLLOW = 0x100;
    private final static int AT_REMOVEDIR = 0x200;
    private final static int AT_EMPTY_PATH = 0x1000;
    
    //Size of block
    private final static int BLOCK_SIZE = 16777216;
    //private final static int BLOCK_SIZE = 1048576;

    private final SysVfs sysVfs;
    private final jnr.ffi.Runtime runtime;

    private final KernelFileHandle rootFh;
    private final int rootFd;

    private final NfsIdMapping idMapper = new SimpleIdMap();
    
    private IgniteCache<String, byte[]> igniteCache;
    
    private Ignite ignite; 

    /**
     * Cache of opened files used by read/write operations.
     */
    private final LoadingCache<Inode, SystemFd> _openFilesCache;

    public LocalVFS(File root, IgniteCache<String, byte[]> cache, Ignite igniteFromStart) throws IOException {

        sysVfs = FFIProvider.getSystemProvider()
                .createLibraryLoader(SysVfs.class)
                .load("c");
        runtime = jnr.ffi.Runtime.getRuntime(sysVfs);

        rootFd = sysVfs.open(root.getAbsolutePath(), O_DIRECTORY, O_RDONLY);
        checkError(rootFd >= 0);

        rootFh = path2fh(rootFd, "", AT_EMPTY_PATH);

        _openFilesCache =  CacheBuilder.newBuilder()
                .maximumSize(1024)
                .removalListener( new FileCloser())
                .build( new FileOpenner() );
        
        //Ignite cache, in memory only, version #1
        igniteCache = cache;
        
        ignite = igniteFromStart;
    }

    @Override
    public int access(Inode inode, int mode) throws IOException {
        // pseudofs will do the checks
        return mode;
    }

    @Override
    public Inode create(Inode parent, Stat.Type type, String path, Subject subject, int mode) throws IOException {
        int uid = (int) Subjects.getUid(subject);
        int gid = (int) Subjects.getPrimaryGid(subject);

        if (type != Stat.Type.REGULAR) {
            throw new NotSuppException("create of this type not supported");
        }

        try (SystemFd fd = inode2fd(parent, O_NOFOLLOW | O_DIRECTORY)) {
            int rfd = sysVfs.openat(fd.fd(), path, O_EXCL | O_CREAT | O_RDWR, mode);
            checkError(rfd >= 0);
            int rc = sysVfs.fchownat(rfd, "", uid, gid, AT_EMPTY_PATH);
            checkError(rc == 0);

            KernelFileHandle fh = path2fh(rfd, "", AT_EMPTY_PATH);
            Inode inode =  fh.toInode();
            _openFilesCache.put(inode, new SystemFd(rfd));
            return inode;
        }
    }

    @Override
    public FsStat getFsStat() throws IOException {
        StatFs statFs = new StatFs(runtime);
        int rc = sysVfs.fstatfs(rootFd, statFs);
        checkError(rc == 0);

        return new FsStat(statFs.f_blocks.get()*statFs.f_bsize.get(),
                statFs.f_files.get(),
                (statFs.f_blocks.get() - statFs.f_bfree.get()) * statFs.f_bsize.get(),
                statFs.f_files.get() - statFs.f_ffree.get());
    }

    @Override
    public Inode getRootInode() throws IOException {
        return rootFh.toInode();
    }

    @Override
    public Inode lookup(Inode parent, String path) throws IOException {
        try(SystemFd fd = inode2fd(parent, O_DIRECTORY | O_PATH )) {
            return path2fh(fd.fd(), path, 0).toInode();
        }
    }

    @Override
    public Inode link(Inode parent, Inode link, String path, Subject subject) throws IOException {
        try (SystemFd dirFd = inode2fd(parent, O_NOFOLLOW | O_DIRECTORY)) {
            try (SystemFd inodeFd = inode2fd(link, O_NOFOLLOW)) {
                int rc = sysVfs.linkat(inodeFd.fd(), "", dirFd.fd(), path, AT_EMPTY_PATH);
                checkError(rc == 0);
                return lookup(parent, path);
            }
        }
    }

    @Override
    public DirectoryStream  list(Inode inode, byte[] verifier, long cookie) throws IOException {

        TreeSet<DirectoryEntry> list = new TreeSet<>();
        try (SystemFd fd = inode2fd(inode, O_DIRECTORY)) {
            Address p = sysVfs.fdopendir(fd.fd());
            checkError(p != null);

            sysVfs.seekdir(p, cookie);

            while (true) {
                Dirent dirent = sysVfs.readdir(p);

                if (dirent == null) {
                    break;
                }

                String name = dirent.getName();
                Inode fInode = path2fh(fd.fd(), name, 0).toInode();
                Stat stat = getattr(fInode);
                list.add(new DirectoryEntry(name, fInode, stat, dirent.d_off.longValue()));
            }
        }
        return new DirectoryStream(DirectoryStream.ZERO_VERIFIER, list);
    }

    @Override
    public byte[] directoryVerifier(Inode inode) throws IOException {
        return DirectoryStream.ZERO_VERIFIER;
    }

    @Override
    public Inode mkdir(Inode parent, String path, Subject subject, int mode) throws IOException {
        int uid = (int) Subjects.getUid(subject);
        int gid = (int) Subjects.getPrimaryGid(subject);

        Inode inode;
        try (SystemFd fd = inode2fd(parent, O_PATH | O_NOFOLLOW | O_DIRECTORY)) {
            int rc = sysVfs.mkdirat(fd.fd(), path, mode);
            checkError(rc == 0);
            inode = lookup(parent, path);
            try (SystemFd fd1 = inode2fd(inode, O_NOFOLLOW | O_DIRECTORY)) {
                rc = sysVfs.fchownat(fd1.fd(), "", uid, gid, AT_EMPTY_PATH );
                checkError(rc == 0);
            }
            return inode;
        }
    }

    @Override
    public boolean move(Inode src, String oldName, Inode dest, String newName) throws IOException {
        try (SystemFd fd1 = inode2fd(src, O_NOFOLLOW | O_DIRECTORY)) {
            try (SystemFd fd2 = inode2fd(dest, O_NOFOLLOW | O_DIRECTORY)) {
                int rc = sysVfs.renameat(fd1.fd(), oldName, fd2.fd(), newName);
                checkError(rc == 0);
                return true;
            }
        }
    }

    @Override
    public Inode parentOf(Inode inode) throws IOException {
        return lookup(inode, "..");
    }

    @Override
    public int read(Inode inode, byte[] data, long offset, int count) throws IOException {
	
	// TODO, this will be different return from pread, how to deal with it?
	int rc = readFromIgnite(inode, data, offset, count);  
	
       // SystemFd fd = getOfLoadRawFd(inode);
       // int rc = sysVfs.pread(fd.fd(), data, count, offset);
      //  checkError(rc >= 0);
        return rc;
    }

    @Override
    public String readlink(Inode inode) throws IOException {
        try (SystemFd fd = inode2fd(inode, O_PATH | O_NOFOLLOW)) {
            Stat stat = statByFd(fd);
            byte[] buf = new byte[(int) stat.getSize()];
            int rc = sysVfs.readlinkat(fd.fd(), "", buf, buf.length);
            checkError(rc >= 0);
            return new String(buf, UTF_8);
        }
    }

    @Override
    public void remove(Inode parent, String path) throws IOException {
        try (SystemFd fd = inode2fd(parent, O_PATH | O_DIRECTORY)) {
            Inode inode = lookup(parent, path);
            Stat stat = getattr(inode);
            int flags = stat.type() == Stat.Type.DIRECTORY ? AT_REMOVEDIR : 0;
            int rc = sysVfs.unlinkat(fd.fd(), path, flags);
            checkError(rc == 0);
        }
    }

    @Override
    public Inode symlink(Inode parent, String path, String link, Subject subject, int mode) throws IOException {
        int uid = (int) Subjects.getUid(subject);
        int gid = (int) Subjects.getPrimaryGid(subject);

        try (SystemFd fd = inode2fd(parent, O_DIRECTORY)) {
            int rc = sysVfs.symlinkat(link, fd.fd(), path);
            checkError(rc == 0);
            Inode inode = lookup(parent, path);
            Stat stat = new Stat();
            stat.setUid(uid);
            stat.setGid(gid);
            try (SystemFd fd1 = inode2fd(inode, O_PATH)) {
                rc = sysVfs.fchownat(fd1.fd(), "", uid, gid, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW);
                checkError(rc == 0);
            }
            return inode;
        }
    }

    @Override
    public WriteResult write(Inode inode, byte[] data, long offset, int count, StabilityLevel stabilityLevel) throws IOException {
    	
    	//Call Write to ignite
    	writeToIgnite(inode, data, offset, count);
        SystemFd fd = getOfLoadRawFd(inode);
        int n = sysVfs.pwrite(fd.fd(), data, count, offset);
        checkError(n >= 0);

        int rc = 0;
        switch (stabilityLevel) {
            case UNSTABLE:
                // NOP
                break;
            case DATA_SYNC:
                rc = sysVfs.fdatasync(fd.fd());
                break;
            case FILE_SYNC:
                rc = sysVfs.fsync(fd.fd());
                break;
            default:
                throw new RuntimeException("bad sync type");
        }
        checkError(rc == 0);
        return new WriteResult(stabilityLevel, n);

    }
    
    public void writeToIgnite (Inode inode, byte[] data, long offset, int count){

    	boolean multipleBlocks = false;
    	int bytesWittenToFirstBlock = count;
    	byte[] firstBlock;
        IgniteTransactions transactions = ignite.transactions();
        boolean aNewBlock = false;
    	
    	int startingBlock = (int) Math.ceil((double)(offset + 1) / BLOCK_SIZE);
        System.out.println("starting block is : " +startingBlock);

        //starting offset is for the block being written to
        int offsetOfStartingBlock = (int) offset % BLOCK_SIZE;
        System.out.println("starting offset " + offsetOfStartingBlock);
        
        if(count > (BLOCK_SIZE - offsetOfStartingBlock))
        {
          multipleBlocks = true;
          bytesWittenToFirstBlock = BLOCK_SIZE - offsetOfStartingBlock;
          System.out.println("i am multiple blocks");
        }
        
        //Write to first Block
       // CacheCompoundKey firstBlockKey = new CacheCompoundKey(inode, startingBlock);
        String firstBlockKey = inode.toString()+startingBlock;
        //if(igniteCache.get(firstBlockKey) != null)
        if(igniteCache.containsKey(firstBlockKey))
        {
      	  firstBlock = igniteCache.get(firstBlockKey);
        }
        else{
//        	int x = 0;
//        	while (x < 1000000){
//        		if(igniteCache.containsKey(firstBlockKey)){
//        			System.out.println("i found the same key while in the loop");
//        			break;
//        		}
//        		x++;
//        	}
      	  firstBlock = new byte[BLOCK_SIZE];
      	  aNewBlock = true;
        }
     
        System.arraycopy(data, 0, firstBlock, (int) offsetOfStartingBlock , bytesWittenToFirstBlock);
        
        //To make sure the write action finished before continue
        try  (Transaction tx = transactions.txStart()) {
            if(aNewBlock) {
        	 System.out.println("This is a new block in ignite, block number: "+ startingBlock +", starting timestamp: "+System.currentTimeMillis() );
            }else {
        	System.out.println("This is a existing block in ignite, block number: "+ startingBlock +", starting timestamp: "+System.currentTimeMillis() );
            }
            igniteCache.put(firstBlockKey, firstBlock);
            tx.commit();
            if(aNewBlock) {
       	 	System.out.println("This is a new block in ignite, block number: "+ startingBlock +", Timestamp after create: "+ System.currentTimeMillis());
           }else {
      	 	System.out.println("This is a existing block in ignite, block number: "+ startingBlock +", Timestamp after create: "+ System.currentTimeMillis());
           }
        }catch (CacheException e) {
            if (e.getCause() instanceof TransactionTimeoutException &&
        	        e.getCause().getCause() instanceof TransactionDeadlockException)

        	        System.out.println(e.getCause().getCause().getMessage());
        }
        
        System.out.println("Write to block :"+startingBlock+" from :"+offsetOfStartingBlock+" to :"+(offsetOfStartingBlock + bytesWittenToFirstBlock - 1));
        
        if (multipleBlocks)
        {
          System.out.println("Bytes written to first block :" + bytesWittenToFirstBlock);

          int byteToBeWrittenToRemaingBlocks = count - bytesWittenToFirstBlock;
          System.out.println("Bytes to be written to remaining blocks : "+ byteToBeWrittenToRemaingBlocks);

          int endingBlock = startingBlock + (int) Math.ceil((double)byteToBeWrittenToRemaingBlocks/BLOCK_SIZE);
          System.out.println("ending block is : "+endingBlock);

          //loop to write to remaining blocks
         int blockBeingWrittenTo = startingBlock + 1;
         int bytesWritten = bytesWittenToFirstBlock;
         int bytesLeftToBeWritten = count - bytesWittenToFirstBlock;
         do {
        	 
        	byte[] nextBlock = new byte[BLOCK_SIZE];
        	//CacheCompoundKey nextBlockKey = new CacheCompoundKey(inode, blockBeingWrittenTo);
        	String nextBlockKey = inode.toString()+blockBeingWrittenTo;
            if(bytesLeftToBeWritten > BLOCK_SIZE)
            {
              System.arraycopy(data, bytesWritten -1, nextBlock, 0 , BLOCK_SIZE);
              System.out.println("Write to block :"+blockBeingWrittenTo+" from :0 to :" + (BLOCK_SIZE -1));
              bytesLeftToBeWritten = bytesLeftToBeWritten - BLOCK_SIZE;
              bytesWritten = bytesWritten + BLOCK_SIZE;
            }
            else{
              System.arraycopy(data, bytesWritten -1, nextBlock, 0 , bytesLeftToBeWritten);
              System.out.println("Write to last block :"+blockBeingWrittenTo+" from :0 to :" + (bytesLeftToBeWritten -1));
            }
            
            try  (Transaction tx = transactions.txStart()) {
        	igniteCache.put(nextBlockKey, nextBlock);
                tx.commit();
            }catch (CacheException e) {
                if (e.getCause() instanceof TransactionTimeoutException &&
            	        e.getCause().getCause() instanceof TransactionDeadlockException)

            	        System.out.println(e.getCause().getCause().getMessage());
            }
            
            blockBeingWrittenTo++;
         } while (blockBeingWrittenTo <= endingBlock );

        }
    }
    
    public int readFromIgnite (Inode inode, byte[] data, long offset, int count) throws IOException{
	int totalByteRead = 0;
	
	boolean multipleBlocks = false;
	int bytesReadFromFirstBlock = count;

	int startingBlock = (int) Math.ceil((double)(offset + 1) / BLOCK_SIZE);
	System.out.println("starting block is : " +startingBlock);

	//starting offset is for the block being read from
	int offsetOfStartingBlock = (int) offset % BLOCK_SIZE;
	System.out.println("starting offset " + offsetOfStartingBlock);

	if(count > (BLOCK_SIZE - offsetOfStartingBlock))
	{
	    multipleBlocks = true;
	    bytesReadFromFirstBlock = BLOCK_SIZE - offsetOfStartingBlock;
	    System.out.println("i am multiple blocks");
	}

	//Read for first block if multi blocks, or the only block needed to be read
	//CacheCompoundKey firstBlockKey = new CacheCompoundKey(inode, startingBlock);
	String firstBlockKey = inode.toString()+startingBlock;
	byte[] firstBlockData = igniteCache.get(firstBlockKey);

	if (firstBlockData != null) {
	    System.out.println("Bytes read for the first block :" + bytesReadFromFirstBlock);
	    System.arraycopy(firstBlockData, offsetOfStartingBlock, data, 0 , bytesReadFromFirstBlock);
	    totalByteRead = bytesReadFromFirstBlock;
	}else {
	    System.out.println("Need read first block from file in file system");
	    SystemFd fd = getOfLoadRawFd(inode);
	    int rc = sysVfs.pread(fd.fd(), data, bytesReadFromFirstBlock, offset);
	    checkError(rc >= 0);
	    
	    totalByteRead = rc; // should be the same as bytesReadFromFirstBlock

	    //write to ignite
	    System.out.println("Write block to cache, length: " +  bytesReadFromFirstBlock);
	    writeToIgnite(inode, data, offset, bytesReadFromFirstBlock);
	}


	//Read the rest of blocks
	if (multipleBlocks)
	{
	    int byteToBeReadFromRemaingBlocks = count - bytesReadFromFirstBlock;
	    System.out.println("Bytes to be read from remaining blocks : "+ byteToBeReadFromRemaingBlocks);

	    int endingBlock = startingBlock + (int) Math.ceil((double)byteToBeReadFromRemaingBlocks/BLOCK_SIZE);
	    System.out.println("ending block is : "+endingBlock);

	    //loop to read remaining blocks
	    int blockBeingReadFrom = startingBlock + 1;
	    int bytesRead = bytesReadFromFirstBlock;
	    int bytesLeftToBeRead = count - bytesReadFromFirstBlock;
	    long offsetInFile = offset+bytesReadFromFirstBlock; 

	    do {
		// CacheCompoundKey nextBlockKey = new CacheCompoundKey(inode, blockBeingReadFrom);
		String nextBlockKey = inode.toString()+ blockBeingReadFrom;
		byte[] nextBlockData = igniteCache.get(nextBlockKey);

		if(bytesLeftToBeRead > BLOCK_SIZE)
		{
		    System.out.println("Read from block :"+blockBeingReadFrom+" from :0 to :" + (BLOCK_SIZE -1));
		    if (nextBlockData != null) {
			System.arraycopy(nextBlockData, 0, data,  bytesRead, BLOCK_SIZE);        	  
		    }else {
			SystemFd fd = getOfLoadRawFd(inode);
			byte[] dataReadFromFile = new byte[BLOCK_SIZE];
			int rc = sysVfs.pread(fd.fd(), dataReadFromFile, BLOCK_SIZE, offsetInFile);
			checkError(rc >= 0);

			System.arraycopy(dataReadFromFile, 0, data,  bytesRead, BLOCK_SIZE);   

			//write to ignite
			writeToIgnite(inode, dataReadFromFile, offsetInFile, BLOCK_SIZE);
		    }

		    bytesLeftToBeRead = bytesLeftToBeRead - BLOCK_SIZE;
		    bytesRead = bytesRead + BLOCK_SIZE;
		    offsetInFile = offsetInFile +BLOCK_SIZE;
		    totalByteRead = totalByteRead + BLOCK_SIZE;
		}else {
		    // last block
		    System.out.println("This is last block to be read from block :"+blockBeingReadFrom+" from :0 to :" + (bytesLeftToBeRead -1));
		    if (nextBlockData != null) {
			System.arraycopy(nextBlockData, 0, data,  bytesRead, bytesLeftToBeRead);        	  
		    }else {
			SystemFd fd = getOfLoadRawFd(inode);
			byte[] dataReadFromFile = new byte[bytesLeftToBeRead];
			int rc = sysVfs.pread(fd.fd(), dataReadFromFile, bytesLeftToBeRead, offsetInFile);
			checkError(rc >= 0);

			System.arraycopy(dataReadFromFile, 0, data,  bytesRead, bytesLeftToBeRead);   

			//write to ignite
			writeToIgnite(inode, dataReadFromFile, offsetInFile, bytesLeftToBeRead);
		    }
		    totalByteRead = totalByteRead + bytesLeftToBeRead;
		}
		blockBeingReadFrom++;
	    } while (blockBeingReadFrom <= endingBlock );  
	}
	
	return totalByteRead;
    }

    @Override
    public void commit(Inode inode, long offset, int count) throws IOException {
        // NOP
    }

    @Override
    public Stat getattr(Inode inode) throws IOException {
        try (SystemFd fd = inode2fd(inode, O_PATH)) {
            return statByFd(fd);
        }
    }

    @Override
    public void setattr(Inode inode, Stat stat) throws IOException {

        int openMode = O_RDONLY;

        Stat currentStat = getattr(inode);
        if (currentStat.type() == Stat.Type.SYMLINK) {
            if (stat.isDefined(Stat.StatAttribute.SIZE)) {
                throw new InvalException("Can't chage size of a symlink");
            }
            openMode = O_PATH | O_RDWR | O_NOFOLLOW;
        }

        if (stat.isDefined(Stat.StatAttribute.SIZE)) {
            openMode |= O_RDWR;
        }

        try (SystemFd fd = inode2fd(inode, openMode)) {
            int uid = -1;
            int gid = -1;
            int rc;

            if ( stat.isDefined(Stat.StatAttribute.OWNER) ) {
                uid = stat.getUid();
            }

            if (stat.isDefined(Stat.StatAttribute.GROUP) ){
                gid = stat.getGid();
            }

            if (uid != -1 || gid != -1) {
                rc = sysVfs.fchownat(fd.fd(), "", uid, gid, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW);
                checkError(rc == 0);
            }

            if (currentStat.type() != Stat.Type.SYMLINK) {
                if (stat.isDefined(Stat.StatAttribute.MODE)) {
                    rc = sysVfs.fchmod(fd.fd(), stat.getMode());
                    checkError(rc == 0);
                }
            }

            if (stat.isDefined(Stat.StatAttribute.SIZE)) {
                rc = sysVfs.ftruncate(fd.fd(), stat.getSize());
                checkError(rc == 0);
            }
        }
    }

    @Override
    public nfsace4[] getAcl(Inode inode) throws IOException {
        return new nfsace4[0];
    }

    @Override
    public void setAcl(Inode inode, nfsace4[] acl) throws IOException {
        // NOP
    }

    @Override
    public boolean hasIOLayout(Inode inode) throws IOException {
        return false;
    }

    @Override
    public AclCheckable getAclCheckable() {
        return AclCheckable.UNDEFINED_ALL;
    }

    @Override
    public NfsIdMapping getIdMapper() {
        return idMapper;
    }

    /**
     * Lookup file handle by path
     *
     * @param fd parent directory open file descriptor
     * @param path path within a directory
     * @param flags ...
     * @return file handle
     * @throws IOException
     */
    private KernelFileHandle path2fh(int fd, String path, int flags) throws IOException {

        int[] mntId = new int[1];
        byte[] bytes = new byte[KernelFileHandle.MAX_HANDLE_SZ];
        ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()).putInt(0, bytes.length);
        int rc = sysVfs.name_to_handle_at(fd, path, bytes, mntId, flags);
        checkError(rc == 0);
        KernelFileHandle fh = new KernelFileHandle(bytes);
        LOG.debug("path = [{}], handle = {}", path, fh);
        return fh;
    }

    private void checkError(boolean condition) throws IOException {

        if (condition) {
            return;
        }

        int errno = runtime.getLastError();
        Errno e = Errno.valueOf(errno);
        String msg = sysVfs.strerror(errno) + " " + e.name() + "(" + errno + ")";
        LOG.debug("Last error: {}", msg);

        switch (e) {
            case ENOENT:
                throw new NoEntException(msg);
            case ENOTDIR:
                throw new NotDirException(msg);
            case EISDIR:
                throw new IsDirException(msg);
            case EIO:
                throw new NfsIoException(msg);
            case ENOTEMPTY:
                throw new NotEmptyException(msg);
            case EEXIST:
                throw new ExistException(msg);
            case ESTALE:
                throw new StaleException(msg);
            case EINVAL:
                throw new InvalException(msg);
            case ENOTSUP:
                throw new NotSuppException(msg);
            case ENXIO:
                throw new NXioException(msg);
            default:
                IOException t = new ServerFaultException(msg);
                LOG.error("unhandled exception ", t);
                throw t;
        }
    }

    private SystemFd inode2fd(Inode inode, int flags) throws IOException {
        KernelFileHandle fh = new KernelFileHandle(inode);
        int fd = sysVfs.open_by_handle_at(rootFd, fh.toBytes(), flags);
        checkError(fd >= 0);
        return new SystemFd(fd);
    }

    private Stat statByFd(SystemFd fd) throws IOException {
        FileStat stat = new FileStat(runtime);
        int rc = sysVfs.__fxstat64(0, fd.fd(), stat);
        checkError(rc == 0);
        return toVfsStat(stat);
    }

    private Stat toVfsStat(FileStat fileStat) {
        Stat vfsStat = new Stat();

        vfsStat.setATime(fileStat.st_atime.get() * 1000);
        vfsStat.setCTime(fileStat.st_ctime.get() * 1000);
        vfsStat.setMTime(fileStat.st_mtime.get() * 1000);

        vfsStat.setGid(fileStat.st_gid.get());
        vfsStat.setUid(fileStat.st_uid.get());
        vfsStat.setDev(fileStat.st_dev.intValue());
        vfsStat.setIno(fileStat.st_ino.intValue());
        vfsStat.setMode(fileStat.st_mode.get());
        vfsStat.setNlink(fileStat.st_nlink.intValue());
        vfsStat.setRdev(fileStat.st_rdev.intValue());
        vfsStat.setSize(fileStat.st_size.get());
        vfsStat.setFileid(fileStat.st_ino.get());
        vfsStat.setGeneration(Math.max(fileStat.st_ctime.get(), fileStat.st_mtime.get()));

        return vfsStat;
    }

    private class FileCloser implements RemovalListener<Inode, SystemFd> {

        @Override
        public void onRemoval(RemovalNotification<Inode, SystemFd> notification) {
            sysVfs.close(notification.getValue().fd());
        }
    }

    private class FileOpenner extends CacheLoader<Inode, SystemFd> {

        @Override
        public SystemFd load(Inode key) throws Exception {
            return inode2fd(key, O_NOFOLLOW | O_RDWR);
        }
    }


    private SystemFd getOfLoadRawFd(Inode inode) throws IOException {
        try {
            return _openFilesCache.get(inode);
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            Throwables.throwIfInstanceOf(t, IOException.class);
            throw new IOException(e.getMessage(), t);
        }
    }

    @SuppressWarnings("PublicInnerClass")
    public interface SysVfs {

        String strerror(int e);

        int open(CharSequence path, int flags, int mode);

        int openat(int dirfd, CharSequence name, int flags, int mode);

        int name_to_handle_at(int fd, CharSequence name, @Out @In byte[] fh, @Out int[] mntId, int flag);

        int open_by_handle_at(int mount_fd, @In byte[] fh, int flags);

        int __fxstat64(int version, int fd, @Transient @Out FileStat fileStat);

        Address fdopendir(int fd);

        int closedir(@In Address dirp);

        void seekdir(@In Address dirp, long offset);

        Dirent readdir(@In @Out Address dirp);

        int readlinkat(int fd, CharSequence path, @Out byte[] buf, int len);

        int unlinkat(int fd, CharSequence path, int flags);

        int close(int fd);

        int mkdirat(int fd, CharSequence path, int mode);

        int fchownat(int fd, CharSequence path, int uid, int gid, int flags);

        int fchmod(int fd, int mode);

        int ftruncate(int fildes, long length);

        int pread(int fd, @Out byte[] buf, int nbyte, long offset);

        int pwrite(int fd, @In byte[] buf, int nbyte, long offset);

        int fsync(int fd);

        int fdatasync(int fd);

        int fstatfs(int fd, @Out StatFs statfs);

        int renameat(int oldfd, CharSequence oldPath , int newfd, CharSequence newPath);

        int symlinkat(CharSequence target, int newdirfd, CharSequence linkpath);

        int linkat(int fd1, CharSequence path1, int fd2, CharSequence path2, int flag);
    }

    /**
     * {@link AutoCloseable} class which represents OS native file descriptor.
     */
    private class SystemFd implements Closeable {

        private final int fd;

        SystemFd(int fd) {
            this.fd = fd;
        }

        int fd() {
            return fd;
        }

        @Override
        public void close() throws IOException {
            int rc = sysVfs.close(fd);
            checkError(rc == 0);
        }
    }
}
