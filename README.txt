Here are the steps to run the code after maven build:
1.) put default-config.xml under:
/tmp/ignite/default-config.xml 

as defined in conf.properties file.

2.) Create an exports file under /etc with line:
/tmp/data *(rw,sec=NONE)

3.) Create a directory /tmp/mnt, and mount it with /tmp/data 
mount -t nfs localhost:/tmp/data /tmp/mnt

4.) Inside project directory, run:  
sudo java -jar target/vfs4j-ignite-1.0-SNAPSHOT-jar-with-dependencies.jar / /etc/exports

5.) You can now copy files from other directories to /tmp/mnt or "cat" existing file in /tmp/mnt to a new file to test the code.
