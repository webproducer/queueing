<?php
namespace Queueing\Pheanstalk;

use Queueing\JobsQueueInterface;
use Pheanstalk\{
    Job,Pheanstalk
};

class JobsQueue implements JobsQueueInterface
{

    /** @var Pheanstalk */
    private $_client = null;

    private $_tubeName = 'default';

    private $_host = '127.0.0.1';
    private $_port = 11300;

    private $_delay = 0;

    /**
     * JobsQueue constructor.
     * @param string $tubeName
     * @param string $host
     * @param string $port
     */
    public function __construct(string $tubeName = 'default', string $host = '127.0.0.1', string $port = '11300') {
        $this->_tubeName = $tubeName;
        $this->_host = $host;
        $this->_port = $port;
    }

    /**
     * @param Pheanstalk $client
     * @return JobsQueueInterface
     */
    public static function createWithClient(Pheanstalk $client) {
        $q = new self();
        $q->_client = $client;
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

    /**
     * @inheritdoc
     */
    public function reserve(int $timeout = null): array {
        $this->_checkConnection();
        $rawJob = $this->_client->reserve($timeout);
        if (!$rawJob) {
            return [0, null];
        }
        return [$rawJob->getId(), $rawJob->getData()];
    }

    /**
     * @inheritdoc
     */
    public function add(
        string $payload,
        int $priority = self::DEFAULT_PRI,
        int $delaySeconds = 0,
        int $ttr = self::DEFAULT_TTR
    ): int {
        $this->_checkConnection();
        return $this->_client->put(
            $payload,
            $priority,
            $delaySeconds,
            $ttr
        );
    }

    /**
     * @inheritDoc
     */
    public function release(int $id, int $delaySeconds = 0) {
        $this->_checkConnection();
        $this->_client->release(
            new Job($id, null),
            self::DEFAULT_PRI,
            $delaySeconds
        );
    }


    /**
     * @inheritdoc
     */
    public function delete(int $id) {
        $this->_checkConnection();
        $this->_client->delete(new Job($id, null));
    }

    /**
     * @inheritdoc
     */
    public function bury(int $id) {
        $this->_checkConnection();
        $this->_client->bury(new Job($id, null));
    }

    private function _checkConnection() {
        if (is_null($this->_client)) {
            $this->_client = new Pheanstalk($this->_host, $this->_port);
            $this->_client->useTube($this->_tubeName);
            $this->_client->watch($this->_tubeName);
        }
    }

}
