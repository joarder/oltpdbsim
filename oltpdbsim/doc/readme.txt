	
	// Cluster capacity
	/**
	 *  For a Consistent Hash Ring of size 2^63 using MD5 hash values 
	 *  converting to unsigned Long numbers can hold up to 8 Exabytes 
	 *  or 8192 Petabytes of Data volume.
	 *  
	 *  If we have 128 fixed number of Partitions, then each Partition can hold up to
	 *  65536 Terabytes of Data volume.
	 *  
	 *  Let, we will have a maximum number of 1,352,100 Data objects in the Cluster 
	 *  where each will consume 1 Megabytes of disk space.
	 *  
	 *  Therefore, each Partition will take nearly 10,563 Data objects 
	 *  or will hold nearly 10.5 Gigabytes of Data volume.
	 *  
	 *  If a Server's maximum capacity is 1 Terabytes and we have 8 physical Servers, 
	 *  then each Server will hold 128/8 = 16 Partitions ~ 16*10.5 = 165 Gigabytes of Data volume.
	 */