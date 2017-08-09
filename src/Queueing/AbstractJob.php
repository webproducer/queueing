<?php
namespace Queueing;


abstract class AbstractJob implements IJob
{
    private $_id = 0;

    private $_data = null;

    /**
     * AbstractJob constructor.
     * @param null|array $data
     * @param int $id
     */
    protected function __construct($data = null, $id = 0) {
        $this->_id = $id;
        $this->_data = $data;
    }

    /**
     * @inheritdoc
     */
    public static function createWithIdAndPayload($id, $payload = null) {
        return new static($payload, intval($id));
    }

    /**
     * @inheritdoc
     */
    public function getPayload() {
        return $this->_data;
    }

    /**
     * @inheritdoc
     */
    public function getId() {
        return $this->_id;
    }

    abstract public function perform();

}
