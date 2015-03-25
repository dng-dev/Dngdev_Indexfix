# Dngdev_Indexfix
This Code increase the performance during full reindexing.

The Idea was to reduce the write load to the innodb transactionlog which is really fast growing in fact of heavy delete update insert queries.

Keep in Mind that this solution can have side effects and only works with mysql.
Tested against Magento 1.8
