# Dngdev_Indexfix
This module increase the performance during full reindexing.

The idea behind was to reduce the write load to innodb transactionlog. Another reason for creating this module was to reclaim used table space inside the innodb buffer pool. The Regular way Magento uses the indexing uses full deletes to tables which is produced heavy IO based load. Also the transaction log grows fast. This module is fixing some queries and uses table rotation for full reindex. 

Keep in Mind that this solution can have side effects and only works with mysql.
Its tested against Magento 1.8 and working in Production
