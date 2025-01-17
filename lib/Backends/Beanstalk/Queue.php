<?php

namespace Queueing\Backends\Beanstalk;

use Queueing\JobsQueueInterface;
use Pheanstalk\{Exception\DeadlineSoonException, Job, Pheanstalk};

class Queue implements JobsQueueInterface
{
    use TimeoutTrait;

    /** @var Pheanstalk */
    private $client = null;
    private $tubeName = 'default';
    private $host = '127.0.0.1';
    private $port = 11300;
    private $ttr = self::DEFAULT_TTR;
    private $delay = 0;

    /**
     * JobsQueue constructor.
     * @param string $tubeName
     * @param string $host
     * @param string $port
     */
    public function __construct(string $tubeName = 'default', string $host = '127.0.0.1', string $port = '11300')
    {
        $this->tubeName = $tubeName;
        $this->host = $host;
        $this->port = $port;
    }

    /**
     * @param Pheanstalk $client
     * @return JobsQueueInterface
     */
    public static function createWithClient(Pheanstalk $client)
    {
        $q = new self();
        $q->client = $client;
        return $q;
    }

    /**
     * @param int $delay Put delay
     * @return self
     */
    public function setDelay($delay)
    {
        $this->delay = $delay;
        return $this;
    }

    /**
     * @param int $ttr Time to process job after reserving
     * @return self
     */
    public function setTtr($ttr)
    {
        $this->ttr = $ttr;
        return $this;
    }

    /**
     * @inheritdoc
     * @throws DeadlineSoonException
     */
    public function reserve(int $timeout = null)
    {
        $this->checkConnection();
        if (is_null($timeout)) {
            $rawJob = $this->client->reserve();
        } else {
            $rawJob = $this->client->reserveWithTimeout($this->millisecondsToSeconds($timeout));
        }
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
    ) {
        $this->checkConnection();
        return $this->client->put(
            $payload,
            $priority,
            $delaySeconds,
            $ttr
        );
    }

    /**
     * @inheritDoc
     */
    public function release(int $id, int $delaySeconds = 0)
    {
        $this->checkConnection();
        $this->client->release(
            new Job($id, null),
            self::DEFAULT_PRI,
            $delaySeconds
        );
    }


    /**
     * @inheritdoc
     */
    public function delete(int $id)
    {
        $this->checkConnection();
        $this->client->delete(new Job($id, null));
    }

    /**
     * @inheritdoc
     */
    public function bury(int $id)
    {
        $this->checkConnection();
        $this->client->bury(new Job($id, null));
    }

    private function checkConnection()
    {
        if (is_null($this->client)) {
            $this->client = Pheanstalk::create($this->host, $this->port);
            $this->client->useTube($this->tubeName);
            $this->client->watchOnly($this->tubeName);
        }
    }
}
