<?php

/**
 * CatalogInventory Stock Status Indexer Resource Model
 *
 * @category    Dngdev
 * @package     Dngdev_Indexer
 * @author      Daniel Niedergesäß <daniel.niedergesaess@gmail.com>
 */
class Dngdev_Indexer_Model_Resource_Indexer_Stock extends Mage_CatalogInventory_Model_Resource_Indexer_Stock
{

    /**
     * Clean up temporary index table
     *
     * magento runs per default delete from table which blows up
     * the mysql transaction log
     *
     */
    public function clearTemporaryIndexTable()
    {
        $this->_getWriteAdapter()->truncateTable($this->getIdxTable());
    }

    /**
     * Synchronize data between index storage and original storage
     *
     * we use here a table rotation instead of deleting the whole table inside an transaction
     * which blows up the mysql transaction log and creates not iops inside the database
     *
     * @return Mage_Index_Model_Resource_Abstract
     */
    public function syncData()
    {
        //create table names for rotation
        $newTableName = $this->getMainTable() . '_new';
        $oldTableName = $this->getMainTable() . '_old';
        //clean up last rotation if there was an error
        $this->_getIndexAdapter()->query(sprintf('DROP TABLE IF EXISTS %s', $newTableName));
        $this->_getIndexAdapter()->query(sprintf('DROP TABLE IF EXISTS %s', $oldTableName));

        try {
            //create new table with same schema like the original one
            $this->_getIndexAdapter()->query(sprintf('CREATE TABLE %s LIKE %s', $newTableName, $this->getMainTable()));

            //CREATE TABLE %s LIKE %s doesn't copy foreign keys so we have to add them
            //foreign keys are unique and have a maximun lenght of 64 characters.
            //so wecreate them with an custom suffix
            //@todo seems dirty needs some refactoring
            $config = $this->_getIndexAdapter()->getConfig();
            $foreignKeys = $this->_getIndexAdapter()->query(
                sprintf('SELECT
                          CONSTRAINT_NAME,
                          COLUMN_NAME,
                          REFERENCED_TABLE_NAME ,
                          REFERENCED_COLUMN_NAME
                         FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                         WHERE TABLE_NAME LIKE \'%s\' AND TABLE_SCHEMA = \'%s\' AND REFERENCED_TABLE_NAME IS NOT NULL;',
                    $this->getMainTable(),
                    $config['dbname']));
            //add foreign keys to new table
            foreach ($foreignKeys->fetchAll() as $fk) {
                $fkName = sprintf(substr($fk['CONSTRAINT_NAME'], 0, strrpos($fk['CONSTRAINT_NAME'], '_')));
                $fkName = strlen($fkName) > 50 ? substr($fkName, 0, 50) : $fkName;
                $fkName = $fkName . '_' . uniqid();

                $this->_getIndexAdapter()->addForeignKey(
                    $fkName, $newTableName, $fk['COLUMN_NAME'], $fk['REFERENCED_TABLE_NAME'], $fk['REFERENCED_COLUMN_NAME']
                );

            }
            //get columns mapping and insert data to new table
            $sourceColumns = array_keys($this->_getWriteAdapter()->describeTable($this->getIdxTable()));
            $targetColumns = array_keys($this->_getWriteAdapter()->describeTable($newTableName));
            $select = $this->_getIndexAdapter()->select()->from($this->getIdxTable(), $sourceColumns);
            $this->insertFromSelect($select, $newTableName, $targetColumns, false);
            //rotate the tables
            $this->_getIndexAdapter()->query(sprintf('RENAME TABLE %s TO %s, %s TO %s',
                $this->getMainTable(),
                $oldTableName,
                $newTableName,
                $this->getMainTable()
            ));
            //drop table to reclaim table space
            $this->_getIndexAdapter()->dropTable($oldTableName);
        } catch (Exception $e) {
            $this->_getIndexAdapter()->query(sprintf('DROP TABLE IF EXISTS %s', $newTableName));
            $this->_getIndexAdapter()->query(sprintf('DROP TABLE IF EXISTS %s', $oldTableName));
            throw $e;
        }
        return $this;
    }
}