<?php

namespace Pimgento\Api\Job;

use Akeneo\Pim\ApiClient\AkeneoPimClientInterface;
use Akeneo\PimEnterprise\ApiClient\AkeneoPimEnterpriseClientInterface;
use Magento\Framework\DataObject;
use Magento\Framework\Event\ManagerInterface;
use Magento\Framework\Phrase;
use Pimgento\Api\Api\Data\ImportInterface;
use Pimgento\Api\Helper\Authenticator;
use Pimgento\Api\Helper\Output as OutputHelper;

/**
 * Class Import
 *
 * @category  Class
 * @package   Pimgento\Api\Job
 * @author    Agence Dn'D <contact@dnd.fr>
 * @copyright 2018 Agence Dn'D
 * @license   http://opensource.org/licenses/osl-3.0.php  Open Software License (OSL 3.0)
 * @link      https://www.pimgento.com/
 */
abstract class Import extends DataObject implements ImportInterface
{
    /**
     * This variable contains a string
     *
     * @var string $code
     */
    protected $code;
    /**
     * This variable contains a string value
     *
     * @var string $name
     */
    protected $name;
    /**
     * This variable contains a string value
     *
     * @var string $identifier
     */
    private $identifier;
    /**
     * This variable contains a boolean
     *
     * @var bool $status
     */
    private $status;
    /**
     * This variable contains an int value
     *
     * @var int $step
     */
    private $step;
    /**
     * This variable contains an array
     *
     * @var array $steps
     */
    private $steps;
    /**
     * This variable contains an OutputHelper
     *
     * @var OutputHelper $outputHelper
     */
    protected $outputHelper;
    /**
     * This variable contains a mixed value
     *
     * @var ManagerInterface $eventManager
     */
    protected $eventManager;
    /**
     * This variable contains a AkeneoPimEnterpriseClientInterface
     *
     * @var AkeneoPimClientInterface|AkeneoPimEnterpriseClientInterface $akeneoClient
     */
    protected $akeneoClient;
    /**
     * This variable contains a string or Phrase value
     *
     * @var string|Phrase $comment
     */
    private $comment;
    /**
     * This variable contains a string or Phrase value
     *
     * @var string|Phrase $message
     */
    private $message;
    /**
     * This variable contains a bool value
     *
     * @var bool $continue
     */
    private $continue;
    /**
     * This variable contains a bool value
     *
     * @var bool $isEnterprise
     */
    protected $isEnterprise = false;
    /**
     * @var \Psr\Log\LoggerInterface $logger
     */
    public $logger;

    /**
     * Import constructor.
     *
     * @param OutputHelper $outputHelper
     * @param ManagerInterface $eventManager
     * @param Authenticator $authenticator
     * @param \Psr\Log\LoggerInterface $logger
     * @param array $data
     */
    public function __construct(
        OutputHelper $outputHelper,
        ManagerInterface $eventManager,
        Authenticator $authenticator,
        \Psr\Log\LoggerInterface $logger,
        array $data = []
    ) {
        parent::__construct($data);

        try {
            $this->akeneoClient = $authenticator->getAkeneoApiClient();
        } catch (\Exception $e) {
            $this->akeneoClient = false;
        }
        $this->logger = $logger;
        $this->outputHelper = $outputHelper;
        $this->eventManager = $eventManager;
        $this->step         = 0;
        $this->initStatus();
        $this->initSteps();
    }

    /**
     * Load steps
     *
     * @return void
     */
    private function initSteps()
    {
        /** @var array $steps */
        $steps = [];
        if ($this->getData('steps')) {
            $steps = $this->getData('steps');
        }

        $this->steps = array_merge(
            [
                [
                    'method'  => 'beforeImport',
                    'comment' => 'Start import',
                ],
            ],
            $steps,
            [
                [
                    'method'  => 'afterImport',
                    'comment' => 'Import complete',
                ],
            ]
        );
    }

    /**
     * Get import code
     *
     * @return string
     */
    public function getCode()
    {
        return $this->code;
    }

    /**
     * Get import name
     *
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * Set import identifier
     *
     * @param string $identifier
     *
     * @return Import
     */
    public function setIdentifier($identifier)
    {
        $this->identifier = $identifier;

        return $this;
    }

    /**
     * Get import identifier
     *
     * @return string
     */
    public function getIdentifier()
    {
        if (!$this->identifier) {
            $this->setIdentifier(uniqid());
        }

        return $this->identifier;
    }

    /**
     * Set current step index
     *
     * @param int $step
     *
     * @return Import
     */
    public function setStep($step)
    {
        $this->step = $step;

        return $this;
    }

    /**
     * Get current step index
     *
     * @return int
     */
    public function getStep()
    {
        return $this->step;
    }

    /**
     * Set import comment
     *
     * @param string|Phrase $comment
     *
     * @return Import
     */
    public function setComment($comment)
    {
        $this->comment = $comment;

        return $this;
    }

    /**
     * Set import message
     *
     * @param string|Phrase $message
     *
     * @return Import
     */
    public function setMessage($message)
    {
        $this->message = $message;

        return $this;
    }

    /**
     * Set import status
     *
     * @param $status
     *
     * @return Import
     */
    public function setStatus($status)
    {
        $this->status = $status;

        return $this;
    }

    /**
     * Get import status
     *
     * @return bool
     */
    public function getStatus()
    {
        return $this->status;
    }

    /**
     * Set continue
     *
     * @param bool $continue
     *
     * @return Import
     */
    public function setContinue($continue)
    {
        $this->continue = $continue;

        return $this;
    }

    /**
     * Get the prefixed comment
     *
     * @return string
     */
    public function getComment()
    {
        return isset($this->steps[$this->getStep()]['comment']) ?
            $this->outputHelper->getPrefix() . $this->steps[$this->getStep()]['comment'] :
            $this->outputHelper->getPrefix() . get_class($this) . '::' . $this->getMethod();
    }

    /**
     * Description getMessage function
     *
     * @return string
     */
    public function getMessage()
    {
        return (string)$this->outputHelper->getPrefix().$this->message;
    }

    /**
     * Get method to execute
     *
     * @return string
     */
    public function getMethod()
    {
        return isset($this->steps[$this->getStep()]['method']) ?
            $this->steps[$this->getStep()]['method'] : null;
    }

    /**
     * Init status, continue and message
     *
     * @return void
     */
    private function initStatus()
    {
        $this->setStatus(true);
        $this->setContinue(true);
        $this->setMessage(__('completed'));
    }

    /**
     * Function called to run import
     * This function will get the right method to call
     *
     * @return array
     */
    public function execute()
    {
        if (!$this->canExecute() || !isset($this->steps[$this->step])) {
            return $this->outputHelper->getImportAlreadyRunningResponse();
        };

        /** @var string $method */
        $method = $this->getMethod();
        if (!method_exists($this, $method)) {
            $this->stop(true);

            return $this->outputHelper->getNoImportFoundResponse();
        }

        if (!$this->akeneoClient) {
            return $this->outputHelper->getApiConnectionError();
        }

        $this->eventManager->dispatch('pimgento_import_step_start', ['import' => $this]);
        $this->eventManager->dispatch(
            'pimgento_import_step_start_'.strtolower($this->getCode()),
            ['import' => $this]
        );
        $this->initStatus();

        try {
            $this->{$method}();
        } catch (\Exception $exception) {
            $this->stop(true);
            $this->setMessage($exception->getMessage());
        }
        /** @var array $response */
        $response = $this->getResponse();

        $this->eventManager->dispatch('pimgento_import_step_finish', ['import' => $this]);
        $this->eventManager->dispatch(
            'pimgento_import_step_finish_'.strtolower($this->getCode()),
            ['import' => $this]
        );

        return $response;
    }

    /**
     * Count steps
     *
     * @return int
     */
    public function countSteps()
    {
        return count($this->steps);
    }

    /**
     * Check if import may be processed (Not already running, ...)
     *
     * @return bool
     */
    public function canExecute()
    {
        if ($this->step < 0 || $this->step > $this->countSteps()) {
            return false;
        }

        return true;
    }

    /**
     * Format data to response structure
     *
     * @return array
     */
    protected function getResponse()
    {
        /** @var array $response */
        $response = [
            'continue'   => $this->continue,
            'identifier' => $this->getIdentifier(),
            'status'     => $this->getStatus(),
        ];

        if ($this->getComment()) {
            $response['comment'] = $this->getComment();
        }

        if ($this->message) {
            $response['message'] = $this->getMessage();
        }

        if (!$this->isDone()) {
            $response['next'] = $this->nextStep()->getComment();
        }

        return $response;
    }

    /**
     * {@inheritdoc}
     */
    public function beforeImport()
    {
        if ($this->akeneoClient === false) {
            $this->setMessage(__('Could not start the import %s, check that your API credentials are correctly configured', $this->getCode()));
            $this->stop(1);

            return;
        }

        /** @var string $identifier */
        $identifier = $this->getIdentifier();

        $this->setMessage(__('Import ID : %1', $identifier));
    }

    /**
     * Function called after any step
     *
     * @return void
     */
    public function afterImport()
    {
        $this->setMessage(__('Import ID : %1', $this->identifier))->stop();
    }

    /**
     * Stop the import (no step will be processed after)
     *
     * @param bool $error
     *
     * @return void
     */
    public function stop($error = false)
    {
        $this->continue = false;
        if ($error == true) {
            $this->setStatus(false);
        }
    }

    /**
     * Description hasError function
     *
     * @return bool
     */
    public function isDone()
    {
        if ($this->continue) {
            return false;
        }

        return true;
    }

    /**
     * Increment the step
     *
     * @return Import
     */
    public function nextStep()
    {
        $this->step += 1;

        return $this;
    }

    /**
     * Retrieve if import is specific from Pim Enterprise
     *
     * @return mixed
     */
    public function isImportEnterprise()
    {
        return $this->isEnterprise;
    }

    /**
     * Pick up the Import job from the specified step.
     * The step's number can be found under its "name" attribute in etc/di.xml
     *
     * @param int $step
     */
    public function runFromStep($step)
    {
        $this->step = $step;
        do  {
            $response = $this->execute();
        } while (
            !empty($response['message'])
            && $response['status']
            && $response['continue']
        );
    }
}
