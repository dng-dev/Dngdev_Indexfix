<?php

/**
 * Catalog Product Eav Select and Multiply Select Attributes Indexer resource model
 *
 * @category    Dngdev
 * @package     Dngdev_Indexer
 * @author      Daniel Niedergesäß <daniel.niedergesaess@gmail.com>
 */
class Dngdev_Indexer_Model_Resource_Product_Indexer_Eav_Source extends Mage_Catalog_Model_Resource_Product_Indexer_Eav_Source
{

    /**
     * Rebuild all index data
     *
     * we simply remove here _removeNotVisibleEntityFromIndex because we can handle this with a join
     *
     * @return Mage_Catalog_Model_Resource_Product_Indexer_Eav_Abstract
     */
    public function reindexAll()
    {
        $this->useIdxTable(true);
        $this->beginTransaction();
        try {
            $this->clearTemporaryIndexTable();
            $this->_prepareIndex();
            $this->_prepareRelationIndex();
            //change start
            #$this->_removeNotVisibleEntityFromIndex();
            //change end

            $this->syncData();
            $this->commit();
        } catch (Exception $e) {
            $this->rollBack();
            throw $e;
        }

        return $this;
    }

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
     * Prepare data index for indexable select attributes
     *
     * added missing where condition for attributes which creates a horrible big temp table on disk
     *
     * @param array $entityIds the entity ids limitation
     * @param int $attributeId the attribute id limitation
     * @return Mage_Catalog_Model_Resource_Product_Indexer_Eav_Source
     */
    protected function _prepareSelectIndex($entityIds = null, $attributeId = null)
    {
        $adapter = $this->_getWriteAdapter();
        $idxTable = $this->getIdxTable();
        // prepare select attributes
        if (is_null($attributeId)) {
            $attrIds = $this->_getIndexableAttributes(false);
        } else {
            $attrIds = array($attributeId);
        }

        if (!$attrIds) {
            return $this;
        }

        /**@var $subSelect Varien_Db_Select */
        $subSelect = $adapter->select()
            ->from(
                array('s' => $this->getTable('core/store')),
                array('store_id', 'website_id')
            )
            ->joinLeft(
                array('d' => $this->getValueTable('catalog/product', 'int')),
                '1 = 1 AND d.store_id = 0',
                array('entity_id', 'attribute_id', 'value')
            )
            //added missing attribute filter
            ->where('s.store_id != 0 and d.attribute_id IN (?)', array_map('intval', $attrIds));

        if (!is_null($entityIds)) {
            $subSelect->where('d.entity_id IN(?)', array_map('intval', $entityIds));
        }

        /**@var $select Varien_Db_Select */
        $select = $adapter->select()
            ->from(
                array('pid' => new Zend_Db_Expr(sprintf('(%s)', $subSelect->assemble()))),
                array()
            )
            ->joinLeft(
                array('pis' => $this->getValueTable('catalog/product', 'int')),
                'pis.entity_id = pid.entity_id AND pis.attribute_id = pid.attribute_id AND pis.store_id = pid.store_id',
                array()
            )
            ->columns(
                array(
                    'pid.entity_id',
                    'pid.attribute_id',
                    'pid.store_id',
                    'value' => $adapter->getIfNullSql('pis.value', 'pid.value')
                )
            )
            ->where('pid.attribute_id IN(?)', $attrIds);

        $select->where(Mage::getResourceHelper('catalog')->getIsNullNotNullCondition('pis.value', 'pid.value'));

        /**
         * Add additional external limitation
         */
        Mage::dispatchEvent('prepare_catalog_product_index_select', array(
            'select' => $select,
            'entity_field' => new Zend_Db_Expr('pid.entity_id'),
            'website_field' => new Zend_Db_Expr('pid.website_id'),
            'store_field' => new Zend_Db_Expr('pid.store_id')
        ));
        $query = $select->insertFromSelect($idxTable);
        $adapter->query($query);
        return $this;
    }

    /**
     * Prepare data index for indexable multiply select attributes
     * @todo this code is solvable via direct sql
     *
     *
     * @param array $entityIds the entity ids limitation
     * @param int $attributeId the attribute id limitation
     * @return Mage_Catalog_Model_Resource_Product_Indexer_Eav_Source
     */
    protected function _prepareMultiselectIndex($entityIds = null, $attributeId = null)
    {
        $adapter = $this->_getWriteAdapter();

        // prepare multiselect attributes
        if (is_null($attributeId)) {
            $attrIds = $this->_getIndexableAttributes(true);
        } else {
            $attrIds = array($attributeId);
        }

        if (!$attrIds) {
            return $this;
        }

        // load attribute options
        $options = array();
        $select = $adapter->select()
            ->from($this->getTable('eav/attribute_option'), array('attribute_id', 'option_id'))
            ->where('attribute_id IN(?)', $attrIds);
        $query = $select->query();
        while ($row = $query->fetch()) {
            $options[$row['attribute_id']][$row['option_id']] = true;
        }

        // prepare get multiselect values query
        $productValueExpression = $adapter->getCheckSql('pvs.value_id > 0', 'pvs.value', 'pvd.value');
        $select = $adapter->select()
            ->from(
                array('pvd' => $this->getValueTable('catalog/product', 'varchar')),
                array('entity_id', 'attribute_id'))
            ->join(
                array('cs' => $this->getTable('core/store')),
                '',
                array('store_id'))
            ->joinLeft(
                array('pvs' => $this->getValueTable('catalog/product', 'varchar')),
                'pvs.entity_id = pvd.entity_id AND pvs.attribute_id = pvd.attribute_id'
                . ' AND pvs.store_id=cs.store_id',
                array('value' => $productValueExpression))
            ->where('pvd.store_id=?',
                $adapter->getIfNullSql('pvs.store_id', Mage_Catalog_Model_Abstract::DEFAULT_STORE_ID))
            ->where('cs.store_id!=?', Mage_Catalog_Model_Abstract::DEFAULT_STORE_ID)
            ->where('pvd.attribute_id IN(?)', $attrIds);

        $statusCond = $adapter->quoteInto('=?', Mage_Catalog_Model_Product_Status::STATUS_ENABLED);
        $this->_addAttributeToSelect($select, 'status', 'pvd.entity_id', 'cs.store_id', $statusCond);

        if (!is_null($entityIds)) {
            $select->where('pvd.entity_id IN(?)', $entityIds);
        }

        /**
         * Add additional external limitation
         */
        Mage::dispatchEvent('prepare_catalog_product_index_select', array(
            'select' => $select,
            'entity_field' => new Zend_Db_Expr('pvd.entity_id'),
            'website_field' => new Zend_Db_Expr('cs.website_id'),
            'store_field' => new Zend_Db_Expr('cs.store_id')
        ));
        $i = 0;
        $data = array();
        $query = $select->query();
        while ($row = $query->fetch()) {
            $values = explode(',', $row['value']);
            foreach ($values as $valueId) {
                if (isset($options[$row['attribute_id']][$valueId])) {
                    $data[] = array(
                        $row['entity_id'],
                        $row['attribute_id'],
                        $row['store_id'],
                        $valueId
                    );
                    $i++;
                    if ($i % 10000 == 0) {
                        $this->_saveIndexData($data);
                        $data = array();
                    }
                }
            }
        }

        $this->_saveIndexData($data);
        unset($options);
        unset($data);
        return $this;
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
            //add join for bypassing disabled products and avoid heavy delete query
            $condition = $this->_getIndexAdapter()->quoteInto('=?',Mage_Catalog_Model_Product_Visibility::VISIBILITY_NOT_VISIBLE);
            $this->_addAttributeToSelect(
                $select,
                'visibility',
                $this->getIdxTable() . '.entity_id',
                $this->getIdxTable() . '.store_id',
                $condition
            );
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