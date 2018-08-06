<?php
namespace Queueing;


class Bulk implements \IteratorAggregate
{

    private $jobs = [];

    /**
     * JobsList constructor.
     * @param array $jobs
     */
    public function __construct(array $jobs)
    {
        foreach ($jobs as $job) {
            $this->addJob($job);
        }
    }

    public function addJob(JobInterface $job): self
    {
        $this->jobs[] = $job;
        return $this;
    }

    /**
     * @return array|JobInterface[]
     */
    public function getJobs(): array
    {
        return $this->jobs;
    }

    /**
     * @inheritDoc
     */
    public function getIterator()
    {
        return new \ArrayIterator($this->jobs);
    }


}
