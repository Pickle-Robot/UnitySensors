using UnityEngine;

using Unity.Burst;
using Unity.Collections;
using Unity.Jobs;
using Unity.Mathematics;

using UnitySensors.DataType.Sensor.PointCloud;

namespace UnitySensors.Sensor.LiDAR
{
    [BurstCompile]
    public struct IRaycastHitsToPointsJob : IJobParallelFor
    {
        [ReadOnly]
        public float minRange;
        [ReadOnly]
        public float sqrMinRange;
        [ReadOnly]
        public float maxRange;
        [ReadOnly]
        public float maxIntensity;
        [ReadOnly, NativeDisableParallelForRestriction]
        public NativeArray<float3> directions;
        [ReadOnly]
        public int indexOffset;
        [ReadOnly]
        public NativeArray<RaycastHit> raycastHits;
        [ReadOnly]
        public NativeArray<float> noises;
        [ReadOnly]
        public Matrix4x4 localToWorldMatrix;
        [ReadOnly]
        public bool outWorldSpace;

        public NativeArray<PointXYZI> points;

        public void Execute(int index)
        {
            float distance = raycastHits[index].distance;
            float distance_noised = distance + noises[index];
            distance = (minRange < distance && distance < maxRange && minRange < distance_noised && distance_noised < maxRange) ? distance_noised : 0;

            float3 position = directions[index + indexOffset] * distance;

            if (outWorldSpace)
            {
                position = math.mul(localToWorldMatrix, new float4(position, 1.0f)).xyz;
            }

            PointXYZI point = new PointXYZI()
            {
                position = position,
                intensity = (distance != 0) ? maxIntensity * sqrMinRange / (distance * distance) : 0
            };
            points[index] = point;
        }
    }
}
