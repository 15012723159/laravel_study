<?php
/**
 * @Notes:
 * @Date: 2026/1/17
 * @Time: 14:16
 * @Interface UcenterNotificationBulkMessageJob
 * @return
 */

namespace App\Jobs;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Redis;
/**
 * User: qinfuxing
 * Date: 2026/1/17
 * Time: 14:16
 */
class UcenterNotificationBulkMessageJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;
    public $queue = 'ucenter_notification_bulk_message';
    public $tries = 1;
    public $timeout = 600; // 10分钟

    protected $batchId;
    protected $messageTemplateId;
    protected $type;
    protected $isForce;
    protected $title;
    protected $metadata;
    protected $filters; // 筛选条件

    public function __construct(
        string $batchId,
        int $messageTemplateId,
        $type,
        $isForce,
    ) {
        $this->batchId = $batchId;
        $this->messageTemplateId = $messageTemplateId;
        $this->type = $type;
        $this->isForce = $isForce;
    }

    public function handle(): void
    {
        Log::channel('api')->info('开始分发批量消息任务', [
            'batch_id' => $this->batchId,
            'messageTemplateId' => $this->messageTemplateId,
        ]);

        // 1. 初始化进度跟踪
        $this->initProgressTracking();

        // 2. 分页查询用户ID，不加载用户数据，只获取ID
        $this->dispatchUserBatchesForStringIds();

        // 3. 标记主任务分发完成
        $this->markDispatchComplete();
    }

    /**
     * 初始化进度跟踪
     */
    protected function initProgressTracking(): void
    {
        // 使用 Redis 存储进度
        Redis::setex("batch:{$this->batchId}:status", 86400, 'dispatching'); // 24小时过期
        Redis::setex("batch:{$this->batchId}:total_users", 86400, 0);
        Redis::setex("batch:{$this->batchId}:processed_users", 86400, 0);
        Redis::setex("batch:{$this->batchId}:created_at", 86400, now()->toDateTimeString());
    }

    /**
     * 分页分发用户批次
     */
    protected function dispatchUserBatchesForStringIds(): void
    {
        $lastId = null; // 字符串ID游标
        $pageSize = 10000; // 每页大小
        $totalUsers = 0;
        $totalBatches = 0;

        // 方法1：使用字符串ID和created_at组合排序
        while (true) {
            $query = DB::connection('mysql_acsdoor')->table('user')->select('id', 'created_at');



            // 字符串ID游标分页逻辑
            if ($lastId) {
                // 使用复合条件：先按created_at，再按id
                $lastUser = DB::table('users')
                    ->select('created_at', 'id')
                    ->where('id', $lastId)
                    ->first();

                if ($lastUser) {
                    $query->where(function($q) use ($lastUser) {
                        $q->where('created_at', '>', $lastUser->created_at)
                            ->orWhere(function($q2) use ($lastUser) {
                                $q2->where('created_at', '=', $lastUser->created_at)
                                    ->where('id', '>', $lastUser->id);
                            });
                    });
                }
            }

            // 排序：先按创建时间，再按ID
            $users = $query->orderBy('created_at')
                ->orderBy('id')
                ->limit($pageSize)
                ->get();

            if ($users->isEmpty()) {
                break;
            }

            // 获取用户ID数组
            $userIds = $users->pluck('id')->toArray();
            $lastUser = $users->last();
            $lastId = $lastUser->id; // 更新游标

            // 分批次处理
            $chunks = array_chunk($userIds, 500);

            foreach ($chunks as $chunk) {
                UcenterNotificationBatchMessage::dispatch(
                    $chunk,
                    $this->messageTemplateId,
                    $this->type,
                    $this->isForce,
                    $this->batchId
                )->onQueue('ucenter_notification_batch_message');

                $totalBatches++;
                $totalUsers += count($chunk);
            }

            // 更新进度
            Redis::set("batch:{$this->batchId}:total_users", $totalUsers);

            Log::channel('api')->info("已分发一批用户", [
                'batch_id' => $this->batchId,
                'last_id' => $lastId,
                'users_in_batch' => count($userIds),
                'total_users_so_far' => $totalUsers,
            ]);

            // 短暂休眠
            usleep(50000); // 0.05秒
        }

        Log::channel('api')->info('字符串ID用户分发完成', [
            'batch_id' => $this->batchId,
            'total_users' => $totalUsers,
            'total_batches' => $totalBatches,
        ]);

        Redis::set("batch:{$this->batchId}:total_users", $totalUsers);
        Redis::set("batch:{$this->batchId}:total_batches", $totalBatches);
    }

    /**
     * 标记分发完成
     */
    protected function markDispatchComplete(): void
    {
        Redis::set("batch:{$this->batchId}:status", 'dispatched');
        Redis::set("batch:{$this->batchId}:dispatched_at", now()->toDateTimeString());

        Log::info('批量消息任务分发完成', [
            'batch_id' => $this->batchId,
        ]);
    }

    /**
     * 任务失败处理
     */
    public function failed(\Throwable $exception): void
    {
        Log::channel('api')->error('分发批量消息任务失败', [
            'batch_id' => $this->batchId,
            'error' => $exception->getMessage(),
        ]);

        Redis::set("batch:{$this->batchId}:status", 'failed');
        Redis::set("batch:{$this->batchId}:error", $exception->getMessage());
    }

}
