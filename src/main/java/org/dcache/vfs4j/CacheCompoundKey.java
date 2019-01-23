package org.dcache.vfs4j;

import org.dcache.nfs.vfs.Inode;

public class CacheCompoundKey {
    public CacheCompoundKey(Inode inode, int blockNum) {
	this.inode = inode;
	this.blockNum = blockNum;
    }
    
    Inode inode;
    int blockNum;


    public Inode getInode() {
	return inode;
    }
    public void setInode(Inode inode) {
	this.inode = inode;
    }
    public int getBlockNum() {
	return blockNum;
    }
    public void setBlockNum(int blockNum) {
	this.blockNum = blockNum;
    }
}
