using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;

namespace RelationalAI.Test
{
    public class UserTest : UnitTest
    {
        public static string Uuid = Guid.NewGuid().ToString();
        public static string UserEmail = $"csharp-sdk-{Uuid}@example.com";

        [Fact]
        public async Task TestUser()
        {
            var client = CreateClient();

            await client
                .Invoking(c => c.FindUserAsync(UserEmail))
                .Should().ThrowAsync<HttpError>();

            var rsp = await client.CreateUserAsync(UserEmail);
            rsp.Email.Should().Be(UserEmail);
            rsp.Status.Should().Be(UserStatus.Active);
            rsp.Roles.Should().Equal(new List<Role> { Role.User });

            var user = await client.GetUserAsync(rsp.Id);
            var userId = user.Id;
            rsp.Id.Should().Be(user.Id);
            user.Email.Should().Be(UserEmail);

            rsp = await client.GetUserAsync(userId);
            rsp.Id.Should().Be(userId);
            rsp.Email.Should().Be(UserEmail);

            rsp = await client.DisableUserAsync(userId);
            rsp.Id.Should().Be(userId);
            rsp.Status.Should().Be(UserStatus.InActive);

            rsp = await client.UpdateUserAsync(userId, UserStatus.InActive);
            rsp.Id.Should().Be(userId);
            rsp.Status.Should().Be(UserStatus.InActive);

            rsp = await client.UpdateUserAsync(userId, UserStatus.Active);
            rsp.Id.Should().Be(userId);
            rsp.Status.Should().Be(UserStatus.Active);

            rsp = await client.UpdateUserAsync(userId, roles: new List<Role> { Role.Admin, Role.User });
            rsp.Id.Should().Be(userId);
            rsp.Roles.Should().Equal(new List<Role> { Role.Admin, Role.User });

            rsp = await client.UpdateUserAsync(userId, UserStatus.InActive, new List<Role> { Role.User });
            rsp.Id.Should().Be(userId);
            rsp.Roles.Should().Equal(new List<Role> { Role.User });

            // cleanup
            await client.DeleteUserAsync(userId);
            rsp.Id.Should().Be(userId);
        }

        public override async Task DisposeAsync()
        {
            var client = CreateClient();

            try
            {
                var oauthClient = await client.FindUserAsync(UserEmail);
                await client.DeleteUserAsync(oauthClient.Id);
            }
            catch (Exception e)
            {
                await Console.Error.WriteLineAsync(e.ToString());
            }
        }
    }
}
