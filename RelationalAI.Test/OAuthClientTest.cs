using System;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;

namespace RelationalAI.Test
{
    public class OAuthClientTests : UnitTest
    {
        public static string Uuid = Guid.NewGuid().ToString();
        public static string OAuthClientName = $"csharp-sdk-{Uuid}";

        [Fact]
        public async Task OAuthClientTest()
        {
            var client = CreateClient();

            await client
                .Invoking(c => c.FindOAuthClientAsync(OAuthClientName))
                .Should().ThrowAsync<HttpError>();

            var rsp = await client.CreateOAuthClientAsync(OAuthClientName);
            rsp.Name.Should().Be(OAuthClientName);

            var clientId = rsp.Id;

            rsp = await client.GetOAuthClientAsync(clientId);
            rsp.Id.Should().Be(clientId);
            rsp.Name.Should().Be(OAuthClientName);

            var deleteRsp = await client.DeleteOAuthClientAsync(clientId);
            deleteRsp.Id.Should().Be(clientId);

            await client
                .Invoking(c => c.GetOAuthClientAsync(clientId))
                .Should().ThrowAsync<HttpError>();
        }

        public override async Task DisposeAsync()
        {
            var client = CreateClient();

            try
            {
                var oauthClient = await client.FindOAuthClientAsync(OAuthClientName);
                await client.DeleteOAuthClientAsync(oauthClient.Id);
            }
            catch (Exception e)
            {
                await Console.Error.WriteLineAsync(e.ToString());
            }
        }
    }
}
