<?php

/**
 * 基于Redis sortedSet实现的延时队列
 * Class DelayQueue
 */
class DelayQueue
{
    private $redis;
    //队列名称
    private $queue_name;

    public function __construct($queue_name)
    {
        $this->redis=new Redis();
        $this->redis->connect('127.0.0.1', 6379);
        $this->queue_name=$queue_name;
    }

    /**
     * 发布消息
     * @param $msg  消息体
     * @param $delay_time 延长时间(单位：秒)
     * @return int
     */
    public function publish($msg,$delay_time)
    {
        $delay_time=time()+$delay_time;
        return $this->redis->zAdd($this->queue_name,$delay_time,$msg);
    }

    /**
     * 消息消费
     */
    public function consume()
    {
        while (true){
            $datas=$this->redis->zRangeByScore($this->queue_name,0,time());
            if(!empty($datas)){
                foreach ($datas as $data)
                {
                    //这里是关键，防止多进程出现重复消费，即只有真正rem的那个进程才算获取到了数据进行下一步消费
                    $success=$this->redis->zRem($this->queue_name,$data);
                    if($success){
                        $this->handle_msg($data);
                    }
                }
            }
            sleep(1);
        }
    }

    /**
     * 处理数据
     * @param $data
     */
    private function handle_msg($data)
    {
        //处理数据
        var_dump($data);
    }
}
