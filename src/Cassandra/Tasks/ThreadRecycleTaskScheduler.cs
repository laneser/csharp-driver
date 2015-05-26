using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra.Tasks
{

    /// <summary>
    /// always new a thread or recycling threads for task.
    /// </summary>
    public class ThreadRecycleScheduler : TaskScheduler
    {
        public static readonly ThreadRecycleScheduler instance = new ThreadRecycleScheduler();

        public static new TaskScheduler Default
        {
            get { return instance; }
        }

        public class RecyclingThreadObj
        {
            public Thread thread { get; set; }

            /// <summary>
            /// when task is null means idling.
            /// </summary>
            public Task task { get; set; }

            /// <summary>
            /// when idling, thread will wait this singal
            /// </summary>
            public EventWaitHandle eventwait { get; set; }

            public int runnedTasks = 0;
        }

        public int MaxIdleThreadCount = Environment.ProcessorCount * 2;

        protected ConcurrentBag<RecyclingThreadObj> IdlingThreads;

        public int PeakIdlingThreadCount = 0;

        public List<RecyclingThreadObj> GetIldingThreads()
        {
            return IdlingThreads.ToList();
        }

        public ThreadRecycleScheduler(int maxIdleThreadCount = -1)
        {
            if (maxIdleThreadCount != -1)
                MaxIdleThreadCount = maxIdleThreadCount;

            IdlingThreads = new ConcurrentBag<RecyclingThreadObj>();

            // monitor too many idling threads.
            var t = new Thread(() =>
            {
                while (true)
                {
                    //logger.Info("peak idling thread is " + PeakIdlingThreadCount);
                    //var idlingTasks = ThreadRecyclingScheduler.instance.GetIldingThreads();
                    //logger.Info("current idling threads :" + idlingTasks.Count);
                    //logger.Info("runnedtasks :" + idlingTasks.Sum(tt => tt.runnedTasks));

                    while (IdlingThreads.Count > MaxIdleThreadCount)
                    {
                        if (PeakIdlingThreadCount < IdlingThreads.Count)
                            PeakIdlingThreadCount = IdlingThreads.Count;

                        RecyclingThreadObj obj;
                        if (IdlingThreads.TryTake(out obj))
                        {
                            obj.thread.Abort();
                            obj.thread.Join();
                            obj.eventwait.Dispose();
                        }
                    }
                    Thread.Sleep(TimeSpan.FromSeconds(10));
                }
            });
            t.IsBackground = true;
            t.Start();
        }

        protected override IEnumerable<Task> GetScheduledTasks()
        {
            // never queue tasks.
            yield break;
        }

        protected override void QueueTask(Task task)
        {
            RecyclingThreadObj obj;
            if (IdlingThreads.TryTake(out obj))
            {
                obj.task = task;
                obj.eventwait.Set();
                return;
            }

            obj = new RecyclingThreadObj();
            obj.task = task;
            obj.eventwait = new EventWaitHandle(true, EventResetMode.AutoReset);
            obj.thread = new Thread(() =>
            {
                while (true)
                {
                    try
                    {
                        if (obj.task != null)
                        {
                            TryExecuteTask(obj.task);
                            obj.runnedTasks++;
                            // we are idling.
                            obj.task = null;
                            IdlingThreads.Add(obj);
                        }
                        obj.eventwait.WaitOne();
                    }
                    catch (ThreadAbortException)
                    { return; }
                    catch (Exception) { }
                }
            });
            obj.thread.IsBackground = true;
            obj.thread.Start();
        }

        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            return TryExecuteTask(task);
        }
    }
}
