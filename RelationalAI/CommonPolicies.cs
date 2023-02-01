/*
 * Copyright 2022 RelationalAI, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Net.Http;
using Polly;

namespace RelationalAI
{
    /// <summary>
    /// Defines a number of commonly used policies and extension methods to use them
    /// to allow retrying operations based on various conditions.
    /// </summary>
    public static class CommonPolicies
    {
        static CommonPolicies()
        {
            RequestErrorResilience = GetRequestErrorResiliencePolicy();
        }

        private static AsyncPolicy RequestErrorResilience { get; }

        /// <summary>
        /// Creates a policy that can be used to produce synchronous API for async calls that may be taking a long
        /// time to complete. Does not time out, therefore use with caution.
        /// Uses exponential back-off starting with a 2 seconds delay up to <paramref name="delayThreshold"/> seconds.
        /// Retries on HTTP request sending errors.
        /// </summary>
        /// <typeparam name="T">Result type of an operation.</typeparam>
        /// <param name="policyBuilder">The policy builder.</param>
        /// <param name="startTime">transaction startTime epoch milliseconds</param>
        /// <param name="overheadRate">the overhead % to add through polling</param>
        /// <param name="delayThreshold">Max delay between retry attempts in seconds</param>
        /// <returns>Resulting policy instance.</returns>
        public static AsyncPolicy<T> RetryForeverWithBoundedDelay<T>(this PolicyBuilder<T> policyBuilder, long startTime, double overheadRate = 0.01, int delayThreshold = 120)
        {
            return policyBuilder
                .WaitAndRetryForeverAsync(retryAttempt => GetBoundedRetryDelay(retryAttempt, startTime, overheadRate, delayThreshold))
                .AddRequestErrorResilience();
        }

        /// <summary>
        /// Creates a policy that can be used to produce synchronous API for async calls which may be taking less than the timeout
        /// to complete, otherwise throws TimeoutRejectedException.
        /// Uses exponential back-off with an overhead % of the time the transaction has been running so far up to 15 seconds and then retrying every 15 seconds.
        /// Retries on HTTP request sending errors.
        /// </summary>
        /// <typeparam name="T">Result type of an operation.</typeparam>
        /// <param name="policyBuilder">The policy builder.</param>
        /// <param name="startTime">transaction startTime epoch milliseconds</param>
        /// <param name="overheadRate">the overhead % to add through polling</param>
        /// <param name="delayThreshold">Max delay between retry attempts in seconds.</param>
        /// <param name="timeoutSeconds">the retry timeout in seconds</param>
        /// <returns>Resulting policy instance.</returns>
        public static AsyncPolicy<T> RetryWithTimeout<T>(this PolicyBuilder<T> policyBuilder, long startTime, double overheadRate, int delayThreshold, int timeoutSeconds)
        {
            return policyBuilder.AddBoundedRetryPolicy(startTime, overheadRate, delayThreshold, timeoutSeconds);
        }

        /// <summary>
        /// Adds a policy that retries 5 times with exponential back-off on HTTP request sending errors,
        /// such as network connectivity issues, or 5xx status codes due to temporary server unavailability.
        /// </summary>
        /// <typeparam name="T">Type of result of an operation.</typeparam>
        /// <param name="policy">Initial policy instance.</param>
        /// <returns>Resulting policy instance.</returns>
        private static AsyncPolicy<T> AddRequestErrorResilience<T>(this IAsyncPolicy<T> policy)
        {
            return policy.WrapAsync(RequestErrorResilience);
        }

        private static AsyncPolicy<T> AddBoundedRetryPolicy<T>(this PolicyBuilder<T> policyBuilder, long startTime, double overheadRate, int delayThreshold, int timeoutSeconds)
        {
            var timeoutPolicy = Policy.TimeoutAsync(TimeSpan.FromSeconds(timeoutSeconds));
            var retryPolicy = policyBuilder.RetryForeverWithBoundedDelay(startTime, overheadRate, delayThreshold);
            return timeoutPolicy.WrapAsync(retryPolicy);
        }

        private static AsyncPolicy GetRequestErrorResiliencePolicy(double overheadRate = 0.01, int maxDelayThreshold = 120)
        {
            var startTime = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            return Policy

                // The request failed due to an underlying issue such as network connectivity, DNS
                // failure, server certificate validation or timeout.
                .Handle<HttpRequestException>()

                // Server error response received (5xx status code, etc.)
                .Or<HttpError>(e => e.StatusCode >= 500)

                // Retry 5 times with overheadRate param of the time the transaction has been running so far
                // And rethrow the exception.
                .WaitAndRetryAsync(5, retryAttempt => GetBoundedRetryDelay(retryAttempt, startTime, overheadRate, maxDelayThreshold));
        }

        // Adds a % overhead of the time the transaction has been running so far
        private static TimeSpan GetBoundedRetryDelay(int retryAttempt, long startTime, double overheadRate, int maxDelayThreshold)
        {
            if (retryAttempt == 1)
            {
                return TimeSpan.FromMilliseconds(500);
            }

            var currentDelay = DateTimeOffset.Now.ToUnixTimeMilliseconds() - startTime; // total run time
            var duration = currentDelay * overheadRate; // overhead rate % of total run time
            return TimeSpan.FromMilliseconds(Math.Min(duration, maxDelayThreshold * 1000));
        }
    }
}
