<?php
namespace Queueing;

class PerformingResult implements PerformingResultInterface
{
    private $errors = [];
    private $completed = [];

    public static function success(JobInterface $job): self
    {
        return (new self)->registerDoneJob($job);
    }

    public static function fail(PerformingException $error): self
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
        $this->errors[] = $error;
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
        return $this->errors;
    }
}
