using System;
using System.Reflection;
using UnityEngine;

using Unity.Collections;
using Unity.Jobs;
using Unity.Mathematics;

using UnitySensors.DataType.Sensor;

using UnitySensors.DataType.Sensor.PointCloud;
using UnitySensors.Utils.Noise;

using Random = Unity.Mathematics.Random;

namespace UnitySensors.Sensor.LiDAR
{
    public class RaycastLiDARSensor360 : LiDARSensor
    {
        [SerializeField]
        private float _scanFrequency = 10.0f; // Hz

        private Transform _transform;

        private JobHandle _jobHandle;

        private IUpdateRaycastCommandsJob _updateRaycastCommandsJob;
        private IUpdateGaussianNoisesJob _updateGaussianNoisesJob;
        private IRaycastHitsToPointsJob _raycastHitsToPointsJob;

        private NativeArray<float3> _directions;
        private NativeArray<RaycastCommand> _raycastCommands;
        private NativeArray<RaycastHit> _raycastHits;

        private NativeArray<float> _noises;

        private PointCloud<PointXYZI> _fullPointCloud; // Accumulator
        private int _currentScanIndex = 0;
        private float _accumulatedTime = 0f;

        public event Action onScanCompleted;

        /// <summary>
        /// Returns the accumulated point cloud containing the full 360 scan in Local Space.
        /// </summary>
        public override PointCloud<PointXYZI> pointCloud { get => _fullPointCloud; }
        public override int pointsNum { get => scanPattern.size; }

        public float ScanFrequency
        {
            get => _scanFrequency;
            set
            {
                _scanFrequency = value;
            }
        }

        protected override void Init()
        {
            base.Init();

            // Note: We don't use base.ApplyScanFrequency or rely on base.Update() logic because 
            // the base logic caps updates to once per frame, which is too slow for 360 scanning 
            // if the batch size is small.

            _transform = this.transform;

            LoadScanData();
            SetupJobs();
        }

        private void LoadScanData()
        {
            _directions = new NativeArray<float3>(scanPattern.size * 2, Allocator.Persistent);
            for (int i = 0; i < scanPattern.size; i++)
            {
                _directions[i] = _directions[i + scanPattern.size] = scanPattern.scans[i];
            }

            // Buffer for accumulation (Local Space)
            _fullPointCloud = new PointCloud<PointXYZI>()
            {
                points = new NativeArray<PointXYZI>(scanPattern.size, Allocator.Persistent)
            };
        }

        private void SetupJobs()
        {
            _raycastCommands = new NativeArray<RaycastCommand>(pointsNumPerScan, Allocator.Persistent);
            _raycastHits = new NativeArray<RaycastHit>(pointsNumPerScan, Allocator.Persistent);
            _noises = new NativeArray<float>(pointsNumPerScan, Allocator.Persistent);

            _updateRaycastCommandsJob = new IUpdateRaycastCommandsJob()
            {
                origin = _transform.position,
                localToWorldMatrix = _transform.localToWorldMatrix,
                maxRange = maxRange,
                directions = _directions,
                indexOffset = 0,
                raycastCommands = _raycastCommands
            };

            _updateGaussianNoisesJob = new IUpdateGaussianNoisesJob()
            {
                sigma = gaussianNoiseSigma,
                random = new Random((uint)Environment.TickCount),
                noises = _noises
            };

            // Calculate points in Local Space directly (outWorldSpace = false)
            _raycastHitsToPointsJob = new IRaycastHitsToPointsJob()
            {
                minRange = minRange,
                sqrMinRange = minRange * minRange,
                maxRange = maxRange,
                maxIntensity = maxIntensity,
                directions = _directions,
                indexOffset = 0,
                raycastHits = _raycastHits,
                noises = _noises,
                points = base.pointCloud.points, // Stores the batch slice
                outWorldSpace = false
            };
        }

        protected override void Update()
        {
            // Override base.Update() to control the simulation rate manually.
            // We want to process 'totalPoints * frequency' points per second.

            _accumulatedTime += Time.deltaTime;

            // Calculate how much time one batch takes (e.g. if we need 100 batches per second, each takes 0.01s)
            // Total batches per second = (TotalPoints / BatchSize) * Frequency
            float batchesPerSecond = (scanPattern.size / (float)pointsNumPerScan) * _scanFrequency;
            float timePerBatch = 1.0f / batchesPerSecond;

            int safetyLoopCount = 0;
            const int MAX_BATCHES_PER_FRAME = 5; // Cap to prevent freezing

            while (_accumulatedTime >= timePerBatch)
            {
                _accumulatedTime -= timePerBatch;
                UpdateSensor();

                safetyLoopCount++;
                if (safetyLoopCount >= MAX_BATCHES_PER_FRAME)
                {
                    // If we are falling behind, reset the accumulator to avoid a death spiral
                    _accumulatedTime = 0;
                    break;
                }
            }
        }

        protected override void UpdateSensor()
        {
            _updateRaycastCommandsJob.origin = _transform.position;
            _updateRaycastCommandsJob.localToWorldMatrix = _transform.localToWorldMatrix;

            JobHandle updateRaycastCommandsJobHandle = _updateRaycastCommandsJob.Schedule(pointsNumPerScan, 1);
            JobHandle updateGaussianNoisesJobHandle = _updateGaussianNoisesJob.Schedule(pointsNumPerScan, 1, updateRaycastCommandsJobHandle);
            JobHandle raycastJobHandle = RaycastCommand.ScheduleBatch(_raycastCommands, _raycastHits, 256, updateGaussianNoisesJobHandle);
            _jobHandle = _raycastHitsToPointsJob.Schedule(pointsNumPerScan, 1, raycastJobHandle);

            JobHandle.ScheduleBatchedJobs();
            _jobHandle.Complete();

            // Copy results from batch buffer to Accumulator
            int remaining = scanPattern.size - _currentScanIndex;
            if (pointsNumPerScan <= remaining)
            {
                NativeArray<PointXYZI>.Copy(base.pointCloud.points, 0, _fullPointCloud.points, _currentScanIndex, pointsNumPerScan);
            }
            else
            {
                // Copy first part to end of buffer
                NativeArray<PointXYZI>.Copy(base.pointCloud.points, 0, _fullPointCloud.points, _currentScanIndex, remaining);
                // Copy second part to start of buffer
                NativeArray<PointXYZI>.Copy(base.pointCloud.points, remaining, _fullPointCloud.points, 0, pointsNumPerScan - remaining);
            }

            // Update offsets for next frame
            int nextIndex = _currentScanIndex + pointsNumPerScan;
            bool scanComplete = nextIndex >= scanPattern.size;

            _currentScanIndex = nextIndex % scanPattern.size;

            _updateRaycastCommandsJob.indexOffset = _currentScanIndex;
            _raycastHitsToPointsJob.indexOffset = _currentScanIndex;

            if (onSensorUpdated != null)
                onSensorUpdated.Invoke();

            if (scanComplete)
            {
                onScanCompleted?.Invoke();
            }
        }

        protected override void OnSensorDestroy()
        {
            _jobHandle.Complete();
            if (_noises.IsCreated) _noises.Dispose();
            if (_directions.IsCreated) _directions.Dispose();
            if (_raycastCommands.IsCreated) _raycastCommands.Dispose();
            if (_raycastHits.IsCreated) _raycastHits.Dispose();

            if (_fullPointCloud != null) _fullPointCloud.Dispose();

            base.OnSensorDestroy();
        }
    }
}
