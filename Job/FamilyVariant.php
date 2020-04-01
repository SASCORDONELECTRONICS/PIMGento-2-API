<?php

namespace Pimgento\Api\Job;

use Akeneo\Pim\ApiClient\Pagination\PageInterface;
use Akeneo\Pim\ApiClient\Pagination\ResourceCursorInterface;
use Magento\Catalog\Api\Data\ProductAttributeInterface;
use Magento\Eav\Model\Config;
use Magento\Framework\App\Cache\Type\Block as BlockCacheType;
use Magento\Framework\App\Cache\TypeListInterface;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\DB\Select;
use Magento\Framework\Event\ManagerInterface;
use Magento\PageCache\Model\Cache\Type as PageCacheType;
use Pimgento\Api\Helper\Authenticator;
use Pimgento\Api\Helper\Config as ConfigHelper;
use Pimgento\Api\Helper\Import\FamilyVariant as FamilyVariantHelper;
use Zend_Db_Expr as Expr;
use Pimgento\Api\Helper\Output as OutputHelper;

/**
 * Class FamilyVariant
 *
 * @category  Class
 * @package   Pimgento\Api\Job
 * @author    Agence Dn'D <contact@dnd.fr>
 * @copyright 2018 Agence Dn'D
 * @license   http://opensource.org/licenses/osl-3.0.php  Open Software License (OSL 3.0)
 * @link      https://www.pimgento.com/
 */
class FamilyVariant extends Import
{
    /**
     * @var int MAX_AXIS_NUMBER
     */
    const MAX_AXIS_NUMBER = 5;
    /**
     * @var string FAMILY_FORK_SUFFIX
     */
    const FAMILY_FORK_SUFFIX = '_fork';
    /**
     * @var string FORKED_ATTRIBUTE_TABLE_NAME
     */
    const FORKED_ATTRIBUTE_TABLE_NAME = 'pimgento_forked_attribute';
    /**
     * This variable contains a string value
     *
     * @var string $code
     */
    protected $code = 'family_variant';
    /**
     * This variable contains a string value
     *
     * @var string $name
     */
    protected $name = 'Family Variant';
    /**
     * This variable contains an FamilyVariantHelper
     *
     * @var FamilyVariantHelper $entitiesHelper
     */
    protected $entitiesHelper;
    /**
     * This variable contains a ConfigHelper
     *
     * @var ConfigHelper $configHelper
     */
    protected $configHelper;
    /**
     * This variable contains a TypeListInterface
     *
     * @var TypeListInterface $cacheTypeList
     */
    protected $cacheTypeList;
    /**
     * This variable contains a Config
     *
     * @var Config $eavConfig
     */
    protected $eavConfig;
    /**
     * @var Attribute $attributeJob
     */
    protected $attributeJob;


    /**
     * FamilyVariant constructor
     *
     * @param FamilyVariantHelper $entitiesHelper
     * @param ConfigHelper        $configHelper
     * @param OutputHelper        $outputHelper
     * @param ManagerInterface    $eventManager
     * @param Authenticator       $authenticator
     * @param TypeListInterface   $cacheTypeList
     * @param Config              $eavConfig
     * @param Attribute           $attributeJob
     * @param  \Psr\Log\LoggerInterface $logger
     * @param array               $data
     */
    public function __construct(
        FamilyVariantHelper $entitiesHelper,
        ConfigHelper $configHelper,
        OutputHelper $outputHelper,
        ManagerInterface $eventManager,
        Authenticator $authenticator,
        \Psr\Log\LoggerInterface $logger,
        TypeListInterface $cacheTypeList,
        Config $eavConfig,
        Attribute $attributeJob,
        array $data = []
    ) {
        parent::__construct($outputHelper, $eventManager, $authenticator, $logger, $data);

        $this->configHelper   = $configHelper;
        $this->entitiesHelper = $entitiesHelper;
        $this->cacheTypeList  = $cacheTypeList;
        $this->eavConfig      = $eavConfig;
        $this->attributeJob   = $attributeJob;
    }

    /**
     * Create temporary table
     *
     * @return void
     */
    public function createTable()
    {
        /** @var PageInterface $families */
        $families = $this->akeneoClient->getFamilyApi()->all();
        /** @var bool $hasVariant */
        $hasVariant = false;
        /** @var array $family */
        foreach ($families as $family) {
            /** @var PageInterface $variantFamilies */
            $variantFamilies = $this->akeneoClient->getFamilyVariantApi()->listPerPage($family['code'], 1);
            if (count($variantFamilies->getItems()) > 0) {
                $hasVariant = true;

                break;
            }
        }
        if (!$hasVariant) {
            $this->setMessage(__('There is no family variant in Akeneo'));
            $this->stop();

            return;
        }
        /** @var array $variantFamily */
        $variantFamily = $variantFamilies->getItems();
        if (empty($variantFamily)) {
            $this->setMessage(__('No results retrieved from Akeneo'));
            $this->stop(1);

            return;
        }
        $variantFamily = reset($variantFamily);
        $this->entitiesHelper->createTmpTableFromApi($variantFamily, $this->getCode());
    }

    /**
     * Insert data into temporary table
     *
     * @return void
     */
    public function insertData()
    {
        /** @var string|int $paginationSize */
        $paginationSize = $this->configHelper->getPanigationSize();
        /** @var PageInterface $families */
        $families = $this->akeneoClient->getFamilyApi()->all($paginationSize);
        /** @var int $count */
        $count = 0;
        /** @var array $family */
        foreach ($families as $family) {
            /** @var string $familyCode */
            $familyCode = $family['code'];
            $count      += $this->insertFamilyVariantData($familyCode, $paginationSize);
        }

        $this->setMessage(
            __('%1 line(s) found', $count)
        );
    }

    /**
     * Update Axis Codes column
     *
     * @return void
     * @throws \Zend_Db_Exception
     */
    public function updateAxisCodes()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());

        // In tmp table, concatenate into a single _axis_code column
        // the contents of all the variant-axes_X columns

        $connection->addColumn($tmpTable, '_axis_codes', [
            'type' => 'text',
            'length' => 255,
            'default' => '',
            'COMMENT' => ' '
        ]);
        /** @var array $columns */
        $columns = [];
        /** @var int $i */
        for ($i = 1; $i <= self::MAX_AXIS_NUMBER; $i++) {
            $columns[] = 'variant-axes_'.$i;
        }
        /**
         * @var int    $key
         * @var string $column
         */
        foreach ($columns as $key => $column) {
            if (!$connection->tableColumnExists($tmpTable, $column)) {
                unset($columns[$key]);
            }
        }

        if (!empty($columns)) {
            /** @var string $update */
            $update = 'TRIM(BOTH "," FROM CONCAT(COALESCE(`' . join('`, \'\' ), "," , COALESCE(`', $columns) . '`, \'\')))';
            $connection->update($tmpTable, ['_axis_codes' => new Expr($update)]);
        }
    }

    /**
     * Enrich axes with new Magento 'select' attributes derived from PIM 'metric' attributes.
     *
     * @throws \Zend_Db_Exception
     * @throws \Zend_Db_Statement_Exception
     */
    public function forkMetricAttributes()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());

        // Make an array of ALL _axis_codes present in tmp_pimgento_entities_family_variant, without duplicates.

        /** @var \Zend_Db_Statement_Interface $variantFamily */
        $variantFamily = $connection->query(
            $connection->select()->from($tmpTable)
        );
        /** @var string $allAxisCodesStr */
        $allAxisCodesStr = '';
        while ($row = $variantFamily->fetch()) {
            $allAxisCodesStr .= $row['_axis_codes'];
        }
        /** @var array $allAxisCodes */
        $allAxisCodesTmp = explode(',', $allAxisCodesStr);
        /** @var array $allAxisCodes */
        $allAxisCodes = array_unique($allAxisCodesTmp);

        // Query Pim API to for all attributes whose type is "metric".
        // $metricAttributes contains K=>V pairs of the type
        // METRIC_ATTRIBUTE_CODE => METRIC_ATTRIBUTE_ARRAY_FROM_API

        $metricAttributes = [];
        $attributes = $this->akeneoClient->getAttributeApi()->all();
        foreach ($attributes as $attribute) {
            if ($attribute['type'] === 'pim_catalog_metric') {
                $metricAttributes[$attribute['code']] = $attribute;
            }
        }

        // Are any of those "metric" attributes present in tmp_pimgento_entities_family_variant?
        // If so, use them to create new attributes with
        // * a new code, patterned after ORIGINAL_CODE + '_FORK'
        // * a new type: 'pim_catalog_simple_select' instead of 'pim_catalog_metric'

        // $newAttributes contains K=>V pairs of the type ORIGINAL_CODE_FORK => FORKED_ATTRIBUTE_ARRAY

        /** @var array $newAttributes */
        $newAttributes = [];
        foreach ($allAxisCodes as $code) {
            if (array_key_exists($code, $metricAttributes)) {

                // Assign those attributes a new code and type.

                $newAttribute = $metricAttributes[$code];
                $newAttribute['code'] = $code . self::FAMILY_FORK_SUFFIX;
                $newAttribute['type'] = 'pim_catalog_simpleselect';
                $newAttributes[$code] = $newAttribute;
            }
        }

        if (count($newAttributes)) {

            // Write those attributes to a fresh tmp_pimgento_entities_attribute table
            $this->entitiesHelper->createTmpTableFromApi(reset($newAttributes), $this->attributeJob->getCode());
            foreach ($newAttributes as $index => $attribute) {
                $this->entitiesHelper->insertDataFromApi($attribute, $this->attributeJob->getCode());
            }

            // Process  tmp_pimgento_entities_attribute as an attribute job,
            // so that the forked attributes are properly imported.
            // Kicking off with "matchEntities()" step, since the table is already created and populated.
            $this->attributeJob->runFromStep(3);

            // In the _axis_codes column of the tmp table, replace the codes of attributes
            // that have been superseded by their "forked" equivalents.
            $variantFamily = $connection->query(
                $connection->select()->from($tmpTable)
            );
            while ($row = $variantFamily->fetch()) {
                /** @var array $rowCodes */
                $axisCodes = explode(',', $row['_axis_codes']);
                $newAxisCodes = [];
                foreach ($axisCodes as $code) {
                    if (array_key_exists($code, $metricAttributes)) {
                        $newAxisCodes[] = $code . self::FAMILY_FORK_SUFFIX;
                    } else {
                        $newAxisCodes[] = $code;
                    }
                }
                $connection->update($tmpTable, ['_axis_codes' => join(',', $newAxisCodes)], ['code = ?' => $row['code']]);
            }

            // Create a new table, pimgento_forked_attribute, to keep track of:
            // * what attributes have been forked;
            // * the names of the forks;
            // This table will come in handy when the Product import job is run.

            $connection = $this->entitiesHelper->getConnection();
            $connection->resetDdlCache(self::FORKED_ATTRIBUTE_TABLE_NAME);
            $connection->dropTable(self::FORKED_ATTRIBUTE_TABLE_NAME);

            $forkedAttributeTable = $connection->newTable(self::FORKED_ATTRIBUTE_TABLE_NAME)
                ->addColumn('code', 'text')
                ->addColumn('code' . self::FAMILY_FORK_SUFFIX, 'text');
            $connection->createTable($forkedAttributeTable);

            foreach ($newAttributes as $originalCode => $newAttribute) {

                // Insert data.
                $connection->insertOnDuplicate(
                    $forkedAttributeTable->getName(),
                    [
                        'code'                              => $originalCode,
                        'code' . self::FAMILY_FORK_SUFFIX   => $newAttribute['code'],
                    ]);
            }
        }
    }

    /**
     * In the tmp table, derive from the _axis_code column
     * an _axis column where variations are identified by their id.
     *
     * @throws \Zend_Db_Statement_Exception
     */
    public function updateAxisIds()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());

        // In the tmp table, derive from the _axis_code column
        // an _axis column where variations are identified by their id.

        $connection->addColumn($tmpTable, '_axis', [
            'type' => 'text',
            'length' => 255,
            'default' => '',
            'COMMENT' => ' '
        ]);

        /** @var array $attributes */
        $attributes = $connection->fetchPairs(
            $connection->select()->from(
                $this->entitiesHelper->getTable('eav_attribute'),
                ['attribute_code', 'attribute_id']
            )->where('entity_type_id = ?', $this->getEntityTypeId())
        );

        /** @var \Zend_Db_Statement_Interface $variantFamily */
        $variantFamily = $connection->query(
            $connection->select()->from($tmpTable)
        );
        while ($row = $variantFamily->fetch()) {
            /** @var array $rowCodes */
            $axisCodes = explode(',', $row['_axis_codes']);
            /** @var array $axis */
            $axis = [];
            /** @var string $code */
            foreach ($axisCodes as $code) {
                if (isset($attributes[$code])) {
                    $axis[] = $attributes[$code];
                }
            }

            $connection->update($tmpTable, ['_axis' => join(',', $axis)], ['code = ?' => $row['code']]);
        }
    }

    /**
     * Update Product Model
     * The pimgento_product_model table is enriched with variation axes.
     *
     * @return void
     */
    public function updateProductModel()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var Select $query */
        $query = $connection->select()->from(false, ['axis' => 'f._axis'])->joinLeft(
            ['f' => $tmpTable],
            'p.family_variant = f.code',
            []
        );

        $connection->query(
            $connection->updateFromSelect($query, ['p' => $this->entitiesHelper->getTable('pimgento_product_model')])
        );
    }

    /**
     * Drop temporary table
     *
     * @return void
     */
    public function dropTable()
    {
        $this->entitiesHelper->dropTable($this->getCode());
    }

    /**
     * Clean cache
     *
     * @return void
     */
    public function cleanCache()
    {
        /** @var array $types */
        $types = [
            BlockCacheType::TYPE_IDENTIFIER,
            PageCacheType::TYPE_IDENTIFIER,
        ];
        /** @var string $type */
        foreach ($types as $type) {
            $this->cacheTypeList->cleanType($type);
        }

        $this->setMessage(
            __('Cache cleaned for: %1', join(', ', $types))
        );
    }

    /**
     * Insert the FamilyVariant data in the temporary table for each family
     *
     * @param string $familyCode
     * @param int    $paginationSize
     *
     * @return int
     */
    protected function insertFamilyVariantData($familyCode, $paginationSize)
    {
        /** @var ResourceCursorInterface $families */
        $families = $this->akeneoClient->getFamilyVariantApi()->all($familyCode, $paginationSize);
        /**
         * @var int   $index
         * @var array $family
         */
        foreach ($families as $index => $family) {
            $this->entitiesHelper->insertDataFromApi($family, $this->getCode());
        }

        if (!isset($index)) {
            return 0;
        }
        $index++;

        return $index;
    }

    /**
     * Get the product entity type id
     *
     * @return string
     */
    protected function getEntityTypeId()
    {
        /** @var string $productEntityTypeId */
        $productEntityTypeId = $this->eavConfig->getEntityType(ProductAttributeInterface::ENTITY_TYPE_CODE)
            ->getEntityTypeId();

        return $productEntityTypeId;
    }
}
