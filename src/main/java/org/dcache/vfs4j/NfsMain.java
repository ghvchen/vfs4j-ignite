package org.dcache.vfs4j;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.dcache.nfs.ExportFile;
import org.dcache.nfs.v3.MountServer;
import org.dcache.nfs.v3.NfsServerV3;
import org.dcache.nfs.v4.MDSOperationFactory;
import org.dcache.nfs.v4.NFSServerV41;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.vfs.VfsCache;
import org.dcache.nfs.vfs.VfsCacheConfig;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.dcache.oncrpc4j.rpc.OncRpcProgram;
import org.dcache.oncrpc4j.rpc.OncRpcSvc;
import org.dcache.oncrpc4j.rpc.OncRpcSvcBuilder;

import java.io.File;
import java.util.Properties;
import java.io.InputStream;

public class NfsMain {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: NfsMain <path> <export file>");
            System.exit(1);
        }
        
        Properties props = new Properties();
        
        InputStream configInput = ClassLoader.getSystemClassLoader().getResourceAsStream("conf.properties");
        	
        props.load(configInput);
        

//        IgniteConfiguration igniteConfiguration = new IgniteConfiguration(); 
//
//        DataStorageConfiguration dataStorageConfig = new DataStorageConfiguration(); 
//
//        long offHeapMemoryMax = 2L * 1024 * 1024 * 1024; 
//
//        DataRegionConfiguration dataRegionConfig = new DataRegionConfiguration(); 
//        dataRegionConfig.setInitialSize((long) Math.ceil(0.25 * offHeapMemoryMax)); // 20% of 2GB
//        dataRegionConfig.setMaxSize(offHeapMemoryMax); // 256MB, for testing purposes 
//        
//        dataRegionConfig.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU); 
//        dataRegionConfig.setEvictionThreshold(0.7); 
//        dataRegionConfig.setEmptyPagesPoolSize(10000); 
//        dataRegionConfig.setName("2GB_Region"); 
//        
//        dataStorageConfig.setDataRegionConfigurations(dataRegionConfig); 
//        igniteConfiguration.setDataStorageConfiguration(dataStorageConfig); 
        

        Ignite ignite = Ignition.start(props.getProperty("cache.ignite.conf"));
        
        //or load default from src/main/resources
        // Ignite ignite = Ignition.start("default-config.xml");
        
        
    //    CacheConfiguration config = new CacheConfiguration("content-access-cache");
        
        //set any config here before start cache
        //config.setCacheMode(CacheMode.PARTITIONED);  //example
        //config.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        //config.setBackups(0);
     //   config.setDataRegionName("2GB_Region");
        
     //   igniteConfiguration.setCacheConfiguration(config); 
        //*** 
        
     //   Ignite ignite = Ignition.start(igniteConfiguration); 
       
	IgniteCache<String, byte[]> cache = ignite.getOrCreateCache("content-access-cache");

        VirtualFileSystem vfs = new LocalVFS( new File(args[0]), cache, ignite);
        OncRpcSvc nfsSvc = new OncRpcSvcBuilder()
                .withPort(2049)
                .withTCP()
                .withAutoPublish()
                .withWorkerThreadIoStrategy()
                .build();

        ExportFile exportFile = new ExportFile( new File(args[1]));

//        VfsCacheConfig cacheConfig = new VfsCacheConfig();
//        cacheConfig.setMaxEntries(1000000);
//        cacheConfig.setLifeTime(5);
//        cacheConfig.setFsStatLifeTime(1);
//        VfsCache vfsCache = new VfsCache(vfs, cacheConfig);
        
        NFSServerV41 nfs4 = new NFSServerV41.Builder()
                .withExportFile(exportFile)
                .withVfs(vfs)
                .withOperationFactory(new MDSOperationFactory())
                .build();

        NfsServerV3 nfs3 = new NfsServerV3(exportFile, vfs);
        MountServer mountd = new MountServer(exportFile, vfs);

        nfsSvc.register(new OncRpcProgram(100003, 3), nfs3);
        nfsSvc.register(new OncRpcProgram(100005, 3), mountd);

        nfsSvc.register(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), nfs4);
        nfsSvc.start();

        Thread.currentThread().join();
    }
}

