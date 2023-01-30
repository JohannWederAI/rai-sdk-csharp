using System;

namespace RelationalAI.Fluent.Exceptions
{
    public class RelClientConfigException : Exception
    {
        public RelClientConfigException(string? message) : base(message)
        {
        }
    }
}