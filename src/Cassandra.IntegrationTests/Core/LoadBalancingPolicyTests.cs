//
//      Copyright (C) 2012 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using NUnit.Framework;
using System;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long")]
    public class LoadBalancingPolicyTests : PolicyTestTools
    {
        protected virtual string IpPrefix
        {
            get
            {
                return "127.0.0.";
            }
        }

        [Test]
        public void RoundRobinTestCCM()
        {
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
            var clusterInfo = TestUtils.CcmSetup(2, builder);
            createSchema(clusterInfo.Session);
            try
            {
                init(clusterInfo, 12);
                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 6);
                assertQueried(IpPrefix + "2", 6);

                resetCoordinators();
                TestUtils.CcmBootstrapNode(clusterInfo, 3);
                TestUtils.waitFor(IpPrefix + "3", clusterInfo.Cluster, 60);

                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 4);
                assertQueried(IpPrefix + "2", 4);
                assertQueried(IpPrefix + "3", 4);

                resetCoordinators();
                TestUtils.CcmDecommissionNode(clusterInfo, 1);
                TestUtils.waitForDecommission(IpPrefix + "1", clusterInfo.Cluster, 60);

                query(clusterInfo, 12);

                assertQueried(IpPrefix + "2", 6);
                assertQueried(IpPrefix + "3", 6);
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void PoliciesAreDifferentInstancesWhenDefault()
        {

            var builder = Cluster.Builder();
            var clusterInfo = TestUtils.CcmSetup(2, builder, null, 2);

            using (var cluster1 = builder.WithConnectionString(String.Format("Contact Points={0}1", IpPrefix)).Build())
            using (var cluster2 = builder.WithConnectionString(String.Format("Contact Points={0}2", IpPrefix)).Build())
            {
                using (var session1 = (Session) cluster1.Connect())
                using (var session2 = (Session) cluster2.Connect())
                {
                    Assert.True(!Object.ReferenceEquals(session1.Policies.LoadBalancingPolicy, session2.Policies.LoadBalancingPolicy), "Load balancing policy instances should be different");
                    Assert.True(!Object.ReferenceEquals(session1.Policies.ReconnectionPolicy, session2.Policies.ReconnectionPolicy), "Reconnection policy instances should be different");
                    Assert.True(!Object.ReferenceEquals(session1.Policies.RetryPolicy, session2.Policies.RetryPolicy), "Retry policy instances should be different");
                }
            }
        }

        [Test]
        public void roundRobinWith2DCsTestCCM()
        {
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
            var clusterInfo = TestUtils.CcmSetup(2, builder, null, 2);
            createSchema(clusterInfo.Session);
            try
            {
                init(clusterInfo, 12);
                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 3);
                assertQueried(IpPrefix + "2", 3);
                assertQueried(IpPrefix + "3", 3);
                assertQueried(IpPrefix + "4", 3);

                resetCoordinators();
                TestUtils.CcmBootstrapNode(clusterInfo, 5, "dc2");
                TestUtils.CcmDecommissionNode(clusterInfo, 1);
                TestUtils.waitFor(IpPrefix + "5", clusterInfo.Cluster, 20);
                TestUtils.waitForDecommission(IpPrefix + "1", clusterInfo.Cluster, 20);

                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 3);
                assertQueried(IpPrefix + "3", 3);
                assertQueried(IpPrefix + "4", 3);
                assertQueried(IpPrefix + "5", 3);
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void DCAwareRoundRobinTestCCM()
        {
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2"));
            var clusterInfo = TestUtils.CcmSetup(2, builder, null, 2);
            createMultiDCSchema(clusterInfo.Session);
            try
            {
                init(clusterInfo, 12);
                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 0);
                assertQueried(IpPrefix + "3", 6);
                assertQueried(IpPrefix + "4", 6);
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void forceStopCCM()
        {
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
            builder.WithQueryTimeout(10000);
            var clusterInfo = TestUtils.CcmSetup(4, builder, null);
            createSchema(clusterInfo.Session);
            try
            {
                init(clusterInfo, 12);
                query(clusterInfo, 12);
                resetCoordinators();
                TestUtils.CcmStopForce(clusterInfo, 1);
                TestUtils.CcmStopForce(clusterInfo, 2);
                TestUtils.waitForDown(IpPrefix + "1", clusterInfo.Cluster, 40);
                TestUtils.waitForDown(IpPrefix + "2", clusterInfo.Cluster, 40);

                query(clusterInfo, 12);

                TestUtils.CcmStopForce(clusterInfo, 3);
                TestUtils.CcmStopForce(clusterInfo, 4);
                TestUtils.waitForDown(IpPrefix + "3", clusterInfo.Cluster, 40);
                TestUtils.waitForDown(IpPrefix + "4", clusterInfo.Cluster, 40);

                try
                {
                    query(clusterInfo, 12);
                    Assert.Fail("It should throw an exception");
                }
                catch (NoHostAvailableException)
                {
                    // No more nodes so ...
                }
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void dcAwareRoundRobinTestWithOneRemoteHostCCM()
        {
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2", 1));
            var clusterInfo = TestUtils.CcmSetup(2, builder, null, 2);
            createMultiDCSchema(clusterInfo.Session);
            try
            {
                init(clusterInfo, 12);
                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 0);
                assertQueried(IpPrefix + "3", 6);
                assertQueried(IpPrefix + "4", 6);
                assertQueried(IpPrefix + "5", 0);

                resetCoordinators();
                TestUtils.CcmBootstrapNode(clusterInfo, 5, "dc3");
                TestUtils.waitFor(IpPrefix + "5", clusterInfo.Cluster, 60);


                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 0);
                assertQueried(IpPrefix + "3", 6);
                assertQueried(IpPrefix + "4", 6);
                assertQueried(IpPrefix + "5", 0);

                resetCoordinators();
                TestUtils.CcmDecommissionNode(clusterInfo, 3);
                TestUtils.CcmDecommissionNode(clusterInfo, 4);
                TestUtils.waitForDecommission(IpPrefix + "3", clusterInfo.Cluster, 20);
                TestUtils.waitForDecommission(IpPrefix + "4", clusterInfo.Cluster, 20);

                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 6);
                assertQueried(IpPrefix + "3", 0);
                assertQueried(IpPrefix + "4", 0);
                assertQueried(IpPrefix + "5", 6);

                resetCoordinators();
                TestUtils.CcmDecommissionNode(clusterInfo, 5);
                TestUtils.waitForDecommission(IpPrefix + "5", clusterInfo.Cluster, 20);

                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 12);
                assertQueried(IpPrefix + "3", 0);
                assertQueried(IpPrefix + "4", 0);
                assertQueried(IpPrefix + "5", 0);

                resetCoordinators();
                TestUtils.CcmDecommissionNode(clusterInfo, 2);
                TestUtils.waitForDecommission(IpPrefix + "2", clusterInfo.Cluster, 20);

                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 12);
                assertQueried(IpPrefix + "2", 0);
                assertQueried(IpPrefix + "3", 0);
                assertQueried(IpPrefix + "4", 0);
                assertQueried(IpPrefix + "5", 0);

                resetCoordinators();
                TestUtils.CcmStopForce(clusterInfo, 1);
                TestUtils.waitForDown(IpPrefix + "2", clusterInfo.Cluster, 20);

                try
                {
                    query(clusterInfo, 12);
                    Assert.Fail();
                }
                catch (NoHostAvailableException)
                {
                    // No more nodes so ...
                }
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void tokenAwareTestCCM()
        {
            tokenAwareTest(false);
        }

        [Test]
        public void tokenAwarePreparedTestCCM()
        {
            tokenAwareTest(true);
        }

        public void tokenAwareTest(bool usePrepared)
        {
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            var clusterInfo = TestUtils.CcmSetup(2, builder);
            createSchema(clusterInfo.Session);
            try
            {
                //clusterInfo.Cluster.RefreshSchema();
                init(clusterInfo, 12);
                query(clusterInfo, 12);

                // Not the best test ever, we should use OPP and check we do it the
                // right nodes. But since M3P is hard-coded for now, let just check
                // we just hit only one node.
                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 12);

                resetCoordinators();
                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 12);

                resetCoordinators();
                TestUtils.CcmStopForce(clusterInfo, 2);
                TestUtils.waitForDown(IpPrefix + "2", clusterInfo.Cluster, 60);

                try
                {
                    query(clusterInfo, 12, usePrepared);
                    Assert.Fail();
                }
                catch (UnavailableException)
                {
                }
                catch (ReadTimeoutException)
                {
                }

                resetCoordinators();
                TestUtils.CcmStart(clusterInfo, 2);
                TestUtils.waitFor(IpPrefix + "2", clusterInfo.Cluster, 60);

                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 12);

                resetCoordinators();
                TestUtils.CcmDecommissionNode(clusterInfo, 2);
                TestUtils.waitForDecommission(IpPrefix + "2", clusterInfo.Cluster, 60);

                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 12);
                assertQueried(IpPrefix + "2", 0);
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void tokenAwareWithRF2TestCCM()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            var clusterInfo = TestUtils.CcmSetup(2, builder);
            createSchema(clusterInfo.Session, 2);
            try
            {

                init(clusterInfo, 12);
                query(clusterInfo, 12);

                // Not the best test ever, we should use OPP and check we do it the
                // right nodes. But since M3P is hard-coded for now, let just check
                // we just hit only one node.
                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 12);
                assertQueried(IpPrefix + "3", 0);

            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }
    }
}