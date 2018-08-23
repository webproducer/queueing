<?php

namespace Queueing\Backends\Beanstalk;

use Amp\Beanstalk\BeanstalkClient;
use Amp\Beanstalk\TimedOutException;
use Amp\Promise;
use Amp\Success;
use Queueing\JobsQueueInterface;
use function Amp\call;

class AsyncQueue implements JobsQueueInterface
{
    /** @var BeanstalkClient */
    private $cli;
    private $host = '127.0.0.1';
    private $port = '11300';
    private $tube = 'default';

    public function __construct(string $tubeName = 'default', string $host = '127.0.0.1', string $port = '11300')
    {
        $this->tube = $tubeName;
        $this->host = $host;
        $this->port = $port;
    }

    /**
     * @inheritDoc
     */
    public function reserve(int $timeout = null)
    {
        return call(function () use ($timeout) {
            /** @var BeanstalkClient $cli */
            $cli = yield $this->getCli();
            try {
                return yield $cli->reserve($timeout);
            } catch (TimedOutException $e) {
                return null;
            }
        });
    }

    /**
     * @inheritDoc
     */
    public function add(
        string $payload,
        int $priority = self::DEFAULT_PRI,
        int $delaySeconds = 0,
        int $ttr = self::DEFAULT_TTR
    ) {
        return call(function () use ($payload, $priority, $delaySeconds, $ttr) {
            /** @var BeanstalkClient $cli */
            $cli = yield $this->getCli();
            return yield $cli->put($payload, $ttr, $delaySeconds, $priority);
        });
    }

    /**
     * @inheritDoc
     */
    public function release(int $id, int $delaySeconds = 0)
    {
        return call(function () use ($id, $delaySeconds) {
            /** @var BeanstalkClient $cli */
            $cli = yield $this->getCli();
            return yield $cli->release($id, $delaySeconds);
        });
    }

    /**
     * @inheritDoc
     */
    public function delete(int $id)
    {
        return call(function () use ($id) {
            /** @var BeanstalkClient $cli */
            $cli = yield $this->getCli();
            return yield $cli->delete($id);
        });
    }

    /**
     * @inheritDoc
     */
    public function bury(int $id)
    {
        return call(function () use ($id) {
            /** @var BeanstalkClient $cli */
            $cli = yield $this->getCli();
            return yield $cli->bury($id);
        });
    }

    private function getCli(): Promise
    {
        if ($this->cli) {
            return new Success($this->cli);
        }
        $this->cli = new BeanstalkClient(sprintf('tcp://%s:%s', $this->host, $this->port));
        if ($this->tube === 'default') {
            return new Success($this->cli);
        }
        return call(function () {
            yield $this->cli->use($this->tube);
            yield $this->cli->watch($this->tube);
            return $this->cli;
        });
    }
}
