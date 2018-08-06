<?php
namespace Queueing;


class PerformingResult implements PerformingResultInterface
{

    private $errros = [];
    private $completed = [];

    public static function success(JobInterface $job): self
    {
        return (new self)->registerDoneJob($job);
    }

    public function fail(PerformingException $error): self
    {
        return (new self)->registerError($error);
    }

    public function registerDoneJob(JobInterface $job): self
    {
        $this->completed[] = $job;
        return $this;
    }

    public function registerError(PerformingException $error): self
    {
        $this->errros[] = $error;
        return $this;
    }

    /**
     * @inheritDoc
     */
    public function getDoneJobs(): array
    {
        return $this->completed;
    }

    /**
     * @inheritDoc
     */
    public function getErrors(): array
    {
        return $this->errros;
    }


}
