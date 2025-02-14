<?php

namespace Queueing;

use Amp\Deferred;
use Amp\Promise;

class WaitGroup implements Promise
{
    private $def;
    private $remain;
    private $isLocked;

    /**
     * WaitGroup constructor.
     * @param int $remain
     * @param bool $isLocked
     */
    public function __construct(int $remain = 0, bool $isLocked = false) {
        $this->def = new Deferred();
        $this->remain = $remain;
        $this->isLocked = $isLocked;
    }

    /**
     * @param int $cnt
     */
    public function done(int $cnt = 1) {
        $this->remain -= $cnt;
        if ($this->isLocked && ($this->remain <= 0)) {
            if (!$this->def->isResolved()) {
                $this->def->resolve();
            }
        }
    }

    /**
     * @param int $cnt
     * @throws Exception
     */
    public function inc(int $cnt = 1) {
        if ($this->isLocked) {
            throw new Exception('WaitGroup is locked');
        }
        $this->remain += $cnt;
    }

    public function lock() {
        $this->isLocked = true;
        if ($this->remain <= 0) {
            $this->def->resolve();
        }
    }

    /**
     * @inheritDoc
     */
    public function onResolve(callable $onResolved) {
        $this->def->promise()->onResolve($onResolved);
    }
}
