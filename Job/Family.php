<?php

namespace Pimgento\Api\Job;

use Akeneo\Pim\ApiClient\Pagination\PageInterface;
use Akeneo\Pim\ApiClient\Pagination\ResourceCursorInterface;
use Magento\Catalog\Api\Data\ProductAttributeInterface;
use Magento\Eav\Model\Config;
use Magento\Eav\Model\Entity\Attribute\Set;
use Magento\Eav\Model\Entity\Attribute\SetFactory;
use Magento\Framework\App\Cache\TypeListInterface;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\DB\Select;
use Magento\Framework\Event\ManagerInterface;
use Pimgento\Api\Helper\Authenticator;
use Pimgento\Api\Helper\Import\Entities;
use Pimgento\Api\Helper\Config as ConfigHelper;
use Zend_Db_Expr as Expr;
use Pimgento\Api\Helper\Output as OutputHelper;

/**
 * Class Family
 *
 * @category  Class
 * @package   Pimgento\Api\Job
 * @author    Agence Dn'D <contact@dnd.fr>
 * @copyright 2018 Agence Dn'D
 * @license   http://opensource.org/licenses/osl-3.0.php  Open Software License (OSL 3.0)
 * @link      https://www.pimgento.com/
 */
class Family extends Import
{
    /**
     * This variable contains a string value
     *
     * @var string $code
     */
    protected $code = 'family';
    /**
     * This variable contains a string value
     *
     * @var string $name
     */
    protected $name = 'Family';
    /**
     * This variable contains an EntitiesHelper
     *
     * @var Entities $entitiesHelper
     */
    protected $entitiesHelper;
    /**
     * This variable contains a ConfigHelper
     *
     * @var ConfigHelper $configHelper
     */
    protected $configHelper;
    /**
     * This variable contains a SetFactory
     *
     * @var SetFactory $attributeSetFactory
     */
    protected $attributeSetFactory;
    /**
     * This variable contains a TypeListInterface
     *
     * @var TypeListInterface $cacheTypeList
     */
    protected $cacheTypeList;
    /**
     * This variable contains an EavConfig
     *
     * @var Config $eavConfig
     */
    protected $eavConfig;

    /**
     * Family constructor
     *
     * @param Entities $entitiesHelper
     * @param ConfigHelper $configHelper
     * @param OutputHelper $outputHelper
     * @param ManagerInterface $eventManager
     * @param Authenticator $authenticator
     * @param \Psr\Log\LoggerInterface $logger
     * @param SetFactory $attributeSetFactory
     * @param TypeListInterface $cacheTypeList
     * @param Config $eavConfig
     * @param array $data
     */
    public function __construct(
        Entities $entitiesHelper,
        ConfigHelper $configHelper,
        OutputHelper $outputHelper,
        ManagerInterface $eventManager,
        Authenticator $authenticator,
        \Psr\Log\LoggerInterface $logger,
        SetFactory $attributeSetFactory,
        TypeListInterface $cacheTypeList,
        Config $eavConfig,

        array $data = []
    ) {
        parent::__construct($outputHelper, $eventManager, $authenticator, $logger, $data);

        $this->configHelper        = $configHelper;
        $this->entitiesHelper      = $entitiesHelper;
        $this->attributeSetFactory = $attributeSetFactory;
        $this->cacheTypeList       = $cacheTypeList;
        $this->eavConfig           = $eavConfig;
    }

    /**
     * Create temporary table for family import
     *
     * @return void
     */
    public function createTable()
    {
        /** @var PageInterface $families */
        $families = $this->akeneoClient->getFamilyApi()->listPerPage(1);
        /** @var array $family */
        $family = $families->getItems();

        if (empty($family)) {
            $this->setMessage(__('No results retrieved from Akeneo'));
            $this->stop(1);

            return;
        }
        $family = reset($family);
        $this->entitiesHelper->createTmpTableFromApi($family, $this->getCode());
    }

    /**
     * Insert families in the temporary table
     *
     * @return void
     */
    public function insertData()
    {
        /** @var string|int $paginationSize */
        $paginationSize = $this->configHelper->getPanigationSize();
        /** @var ResourceCursorInterface $families */
        $families = $this->akeneoClient->getFamilyApi()->all($paginationSize);
        /**
         * @var int $index
         * @var array $family
         */
        foreach ($families as $index => $family) {
            $this->entitiesHelper->insertDataFromApi($family, $this->getCode());
        }
        $index++;

        $this->setMessage(
            __('%1 line(s) found', $index)
        );
    }

    /**
     * Match code with entity
     *
     * @return void
     */
    public function matchEntities()
    {
        $this->entitiesHelper->matchEntity('code', 'eav_attribute_set', 'attribute_set_id', $this->getCode());
    }

    /**
     * Insert families
     *
     * @return void
     */
    public function insertFamilies()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var string $label */
        $label = 'labels-'.$this->configHelper->getDefaultLocale();
        /** @var string $productEntityTypeId */
        $productEntityTypeId = $this->eavConfig->getEntityType(ProductAttributeInterface::ENTITY_TYPE_CODE)
            ->getEntityTypeId();
        /** @var array $values */
        $values = [
            'attribute_set_id'   => '_entity_id',
            'entity_type_id'     => new Expr($productEntityTypeId),
            'attribute_set_name' => new Expr('CONCAT("Pim", " ", `' . $label . '`)'),
            'sort_order'         => new Expr(1),
        ];
        /** @var Select $families */
        $families = $connection->select()->from($tmpTable, $values);

        $connection->query(
            $connection->insertFromSelect(
                $families,
                $this->entitiesHelper->getTable('eav_attribute_set'),
                array_keys($values),
                1
            )
        );
    }

    /**
     * Insert relations between family and list of attributes
     *
     * @return void
     * @throws \Zend_Db_Statement_Exception
     */
    public function insertFamiliesAttributeRelations()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var string $familyAttributeRelationsTable */
        $familyAttributeRelationsTable = $this->entitiesHelper->getTable('pimgento_family_attribute_relations');

        $connection->delete($familyAttributeRelationsTable);
        /** @var array $values */
        $values = [
            'family_entity_id' => '_entity_id',
            'attribute_code'   => 'attributes',
        ];
        /** @var Select $relations */
        $relations = $connection->select()->from($tmpTable, $values);
        /** @var \Zend_Db_Statement_Interface $query */
        $query = $connection->query($relations);
        /** @var array $row */
        while ($row = $query->fetch()) {
            /** @var array $attributes */
            $attributes = explode(',', $row['attribute_code']);
            /** @var string $attribute */
            foreach ($attributes as $attribute) {
                $connection->insert(
                    $familyAttributeRelationsTable,
                    ['family_entity_id' => $row['family_entity_id'], 'attribute_code' => $attribute]
                );
            }
        }
    }

    /**
     * Init group
     *
     * @return void
     * @throws \Exception
     * @throws \Zend_Db_Statement_Exception
     */
    public function initGroup()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var \Zend_Db_Statement_Interface $query */
        $query = $connection->query(
            $connection->select()->from($tmpTable, ['_entity_id'])->where('_is_new = ?', 1)
        );
        /** @var string $defaultAttributeSetId */
        $defaultAttributeSetId = $this->eavConfig->getEntityType(ProductAttributeInterface::ENTITY_TYPE_CODE)
            ->getDefaultAttributeSetId();
        /** @var int $count */
        $count = 0;
        /** @var array $row */
        while (($row = $query->fetch())) {
            /** @var Set $attributeSet */
            $attributeSet = $this->attributeSetFactory->create();
            $attributeSet->load($row['_entity_id']);

            if ($attributeSet->hasData()) {
                $attributeSet->initFromSkeleton($defaultAttributeSetId)->save();
            }
            $count++;
        }

        $this->setMessage(
            __('%1 family(ies) initialized', $count)
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
        /** @var string[] $types */
        $types = [
            \Magento\Framework\App\Cache\Type\Block::TYPE_IDENTIFIER,
            \Magento\PageCache\Model\Cache\Type::TYPE_IDENTIFIER,
        ];
        /** @var string $type */
        foreach ($types as $type) {
            $this->cacheTypeList->cleanType($type);
        }

        $this->setMessage(
            __('Cache cleaned for: %1', join(', ', $types))
        );
    }
}
