<?php
namespace Queueing\Console;

use Queueing\{ BaseFactory, IJobFactory, IJobPerformer, IJobsQueue, QueueProcessor };
use Queueing\Pheanstalk\JobsQueue;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{ InputDefinition, InputInterface, InputOption };
use Symfony\Component\Console\Output\OutputInterface;

class ProcessQueueCommand extends Command
{

    /** @var IJobPerformer */
    private $performer;

    /** @var IJobFactory */
    private $factory;

    private $terminated = false;

    public function __construct($name = null) {
        parent::__construct($name);
        $this->factory = new BaseFactory();
    }

    public function setPerformer(IJobPerformer $performer) {
        $this->performer = $performer;
        return $this;
    }

    public function setFactory(IJobFactory $f) {
        $this->factory = $f;
        return $this;
    }

    protected function configure() {
        $this->setName("wp:process-queue")
            ->setDescription("Run queue processing script")
            ->addOption(
                'backend',
                'b',
                InputOption::VALUE_REQUIRED,
                'Queue managing backend',
                'beanstalk://127.0.0.1:11300/?queue=default'
            );
    }

    protected function execute(InputInterface $input, OutputInterface $output) {
        if (!$this->performer) {
            throw new \RuntimeException("You must set instance of IJobPerformer");
        }
        $p = new QueueProcessor();
        $p->setJobFactory($this->factory);
        $p->setJobPerformer($this->performer);
        $q = $this->makeQueue($input->getOption('backend'));
        $this->sigSetup();
        foreach ($p->process($q) as $job) {
            $this->sigDispatch();
            if ($this->terminated) {
                break;
            }
        }
    }

    /**
     * @param string $desc
     * @return IJobsQueue
     */
    private function makeQueue($desc) {
        $dsn = parse_url($desc);
        switch ($dsn['scheme']) {
            case 'beanstalk':
                $queue = 'default';
                if (isset($dsn['query'])) {
                    parse_str($dsn['query'], $q);
                    $queue = $q['queue'] ?? 'default';
                }
                return new JobsQueue($queue, $dsn['host'], $dsn['port'] ?? 11300);
            default:
                throw new \InvalidArgumentException("Unknown backend: {$dsn['scheme']}");
        }
    }

    private function sigSetup() {
        $stopSigHandler = function($sig) {
            fprintf(STDERR, "Got signal %s. Exiting...\n", $sig);
            $this->terminated = true;
        };
        pcntl_signal(SIGTERM, $stopSigHandler);
        pcntl_signal(SIGINT, $stopSigHandler);
    }

    private function sigDispatch() {
        pcntl_signal_dispatch();
    }


}
