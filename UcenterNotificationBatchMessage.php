<?php
/**
 * @Notes:
 * @Date: 2026/1/17
 * @Time: 14:25
 * @Interface UcenterNotificationBatchMessage
 * @return
 */

namespace App\Jobs;

use App\Models\Message\UcenterMessageNotification;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Bus\Queueable;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\DB;
/**
 * User: qinfuxing
 * Date: 2026/1/17
 * Time: 14:25
 */
class UcenterNotificationBatchMessage  implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    // Redis 队列配置
    //public $queue = '';
    //public $connection = 'redis';

    // 队列属性
    public $tries = 3;                    // 重试次数
    public $maxExceptions = 1;            // 最大异常次数
    public $timeout = 300;                // 5分钟超时
    public $backoff = [60, 120, 300];     // 重试间隔（秒）

    // 批量大小
    public $chunkSize = 500;

    // 任务数据
    protected $batchId;
    protected $userIds;

    protected $type;

    protected $isForce;
    protected $messageTemplateId ;

    /**
     * 创建批量记录任务
     */
    public function __construct(
        array $userIds,
        string $messageTemplateId,
        int $type,
        int $isForce,
    ) {
        $this->userIds = $userIds;
        $this->messageTemplateId = $messageTemplateId;
        $this->type = $type;
        $this->isForce = $isForce;
        $this->batchId = $this->generateBatchId();
    }

    /**
     * 执行任务 - 批量记录消息
     */
    public function handle(): void
    {
        $startTime = microtime(true);
        $totalUsers = count($this->userIds);

        Log::channel('api')->info('开始批量记录消息', [
            'batch_id' => $this->batchId,
            'message_template_id' => $this->messageTemplateId,
            'type' => $this->type,
            'is_force' => $this->isForce,
            'total_users' => $totalUsers,
            'job_id' => $this->job->getJobId(),
        ]);

        try {
            // 分块处理用户，每块500个
            $chunks = array_chunk($this->userIds, $this->chunkSize);
            $totalInserted = 0;

            foreach ($chunks as $chunkIndex => $chunkUserIds) {
                $chunkStartTime = microtime(true);

                $inserted = $this->processChunk($chunkUserIds, $chunkIndex);
                $totalInserted += $inserted;

                $chunkTime = microtime(true) - $chunkStartTime;

                Log::channel('api')->debug("处理分块 {$chunkIndex}", [
                    'batch_id' => $this->batchId,
                    'chunk_index' => $chunkIndex,
                    'chunk_size' => count($chunkUserIds),
                    'inserted' => $inserted,
                    'time_seconds' => round($chunkTime, 3),
                ]);

                // 短暂休眠，避免数据库压力过大
                if ($chunkIndex < count($chunks) - 1) {
                    usleep(50000); // 0.05秒
                }
            }

            $totalTime = microtime(true) - $startTime;

            Log::channel('api')->info('批量记录消息完成', [
                'batch_id' => $this->batchId,
                'total_inserted' => $totalInserted,
                'total_users' => $totalUsers,
                'total_time_seconds' => round($totalTime, 3),
                'performance' => [
                    'users_per_second' => round($totalInserted / max($totalTime, 0.001), 1),
                    'records_per_second' => round($totalInserted / max($totalTime, 0.001), 1),
                ],
            ]);

        } catch (\Exception $e) {
            Log::channel('api')->error('批量记录消息失败', [
                'batch_id' => $this->batchId,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);

            // 标记任务失败，让队列重试
            $this->fail($e);
        }
    }

    /**
     * 处理单个分块（500个用户）
     */
    protected function processChunk(array $userIds, int $chunkIndex): int
    {
        $records = [];
        $now = now();

        foreach ($userIds as $userId) {
            // 个性化内容（可选）
            $records[] = [
                'user_id' => $userId,
                'message_template_id' => $this->messageTemplateId,
                'type' => $this->type,
                'is_force' => $this->isForce,
               // 'batch_id' => $this->batchId,
                'created_at' => $now,
                'updated_at' => $now,
            ];

            // 每500条记录插入一次
            if (count($records) >= 500) {
                $this->bulkInsert($records);
                $records = [];
            }
        }

        // 插入剩余记录
        if (!empty($records)) {
            $this->bulkInsert($records);
        }

        return count($userIds);
    }

    /**
     * 批量插入记录（高性能）
     */
    protected function bulkInsert(array &$records): void
    {
        try {
            // 使用事务确保数据一致性
            DB::transaction(function () use ($records) {
                // 使用 insert 而不是 create，性能更高
                UcenterMessageNotification::insert($records);
            });

        } catch (\Exception $e) {
            Log::channel('api')->warning('批量插入失败，尝试逐条插入', [
                'batch_id' => $this->batchId,
                'error' => $e->getMessage(),
                'record_count' => count($records),
            ]);

            // 回退方案：逐条插入
            $this->insertOneByOne($records);
        }
    }

    /**
     * 逐条插入（回退方案）
     */
    protected function insertOneByOne(array $records): void
    {
        foreach ($records as $record) {
            try {
                UcenterMessageNotification::create($record);
            } catch (\Exception $e) {
                Log::channel('api')->error('插入单条消息记录失败', [
                    'batch_id' => $this->batchId,
                    'user_id' => $record['user_id'],
                    'error' => $e->getMessage(),
                ]);

                // 继续处理其他记录，不中断
                continue;
            }
        }
    }

    /**
     * 生成批次ID
     */
    protected function generateBatchId(): string
    {
        return 'ucenter_notify_message_' . date('Ymdhis') . '_' . uniqid("", true);
    }

    /**
     * 获取批次ID（可用于追踪）
     */
    public function getBatchId(): string
    {
        return $this->batchId;
    }

}
