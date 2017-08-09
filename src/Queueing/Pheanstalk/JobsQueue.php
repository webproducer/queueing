<?php
namespace Queueing\Pheanstalk;

use Queueing\IJobsQueue;
use Queueing\IJob;
use Queueing\JobCreatingException;
use Pheanstalk\Job;
use Pheanstalk\PheanstalkInterface;
use Pheanstalk\Pheanstalk;

class JobsQueue implements IJobsQueue
{

    const DEFAULT_TTR = 600; // 10 minutes

    /** @var Pheanstalk */
    private $_client = null;

    private $_tubeName = 'default';

    private $_host = '127.0.0.1';
    private $_port = 11300;

    private $_delay = 0;
    private $_ttr = self::DEFAULT_TTR;


    private function __construct() {

    }

    public static function createWithClient(Pheanstalk $client) {
        $q = new self();
        $q->_client = $client;
        return $q;
    }

    /**
     * @param string $tubeName
     * @param string $host
     * @param int $port
     * @return JobsQueue
     */
    public static function create($tubeName = 'default', $host = '127.0.0.1', $port = 11300) {
        $q = new self();
        $q->_tubeName = $tubeName;
        $q->_host = $host;
        $q->_port = $port;
        return $q;
    }

    /**
     * @param int $delay Put delay
     * @return self
     */
    public function setDelay($delay) {
        $this->_delay = $delay;
        return $this;
    }

    /**
     * @param int $ttr Time to process job after reserving
     * @return self
     */
    public function setTtr($ttr) {
        $this->_ttr = $ttr;
        return $this;
    }

    public function reserve($timeout = null) {
        $this->_checkConnection();
        $rawJob = $this->_client->reserve($timeout);
        if (!$rawJob) {
            return null;
        }
        $rawData = json_decode($rawJob->getData(), true);
        if (!isset($rawData['class'])) {
            $this->_client->bury($rawJob);
            throw new JobCreatingException("Job class is not specified. Buried.");
        }
        if (!is_subclass_of($rawData['class'], IJob::class, true)) {
            $this->_client->bury($rawJob);
            throw new JobCreatingException("Class {$rawData['class']} doesn't implement IJob interface. Buried.");
        }
        $payload = isset($rawData['payload']) ? $rawData['payload'] : null;
        return call_user_func_array(
            [$rawData['class'], 'createWithIdAndPayload'],
            [$rawJob->getId(), $payload]
        );
    }

    public function add(IJob $job) {
        $this->_checkConnection();
        $data = [
            'class' => get_class($job),
            'payload' => $job->getPayload()
        ];
        $this->_client->put(
            json_encode($data),
            PheanstalkInterface::DEFAULT_PRIORITY,
            $this->_delay,
            $this->_ttr
        );
    }

    public function delete(IJob $job) {
        $this->_checkConnection();
        $this->_client->delete(new Job($job->getId(), null));
    }

    public function bury(IJob $job) {
        $this->_checkConnection();
        $this->_client->bury(new Job($job->getId(), null));
    }

    private function _checkConnection() {
        if (is_null($this->_client)) {
            $this->_client = new Pheanstalk($this->_host, $this->_port);
            $this->_client->useTube($this->_tubeName);
            $this->_client->watch($this->_tubeName);
        }
    }

}
