<?php
namespace Queueing;


class FactoryFunc implements JobFactoryInterface
{

    private $f;

    /**
     * FactoryFunc constructor.
     * @param \Closure $f - Factory func (int $id, string $payload)
     */
    public function __construct(\Closure $f) {
        $this->f = $f;
    }

    /**
     * @inheritDoc
     */
    public function makeJob(int $id, string $payload): JobInterface {
        return call_user_func_array($this->f, [$id, $payload]);
    }


}
