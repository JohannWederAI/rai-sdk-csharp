using System;
using System.Collections;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace RelationalAI.Fluent
{
    public interface INeedResource
    {
        INeedResource UseResource(string name);
        RelQueryBuilder ToBuilder();
    }

    public class RelQueryBuilder : INeedResource
    {
        private Assembly _assembly;
        private ArrayList _resources = new ArrayList();
        private StringBuilder _sb = new StringBuilder();

        private RelQueryBuilder()
        {
        }

        public static INeedResource FromAssembly(Assembly assembly)
        {
            return new RelQueryBuilder
            {
                _assembly = assembly
            };
        }

        public static RelQueryBuilder FromResource(Assembly assembly, string resourceName)
        {
            return FromAssembly(assembly)
                .UseResource(resourceName)
                .ToBuilder();
        }

        public INeedResource UsingAssembly(Assembly assembly)
        {
            _assembly = assembly;
            return this;
        }

        public INeedResource UseResource(string resourceName)
        {
            var resource = _assembly.GetManifestResourceNames()
                .Single(r => r.ToLower().EndsWith(resourceName.ToLower()));

            using (var stream = _assembly.GetManifestResourceStream(resource))
            {
                using (var reader = new StreamReader(stream))
                {
                    var result = reader.ReadToEnd();
                    _sb.AppendLine(result);
                    _sb.AppendLine();
                }
            }

            return this;
        }

        public RelQueryBuilder UseResource(Assembly assembly, string resourceName)
        {
            return UsingAssembly(assembly)
                .UseResource(resourceName)
                .ToBuilder();
        }

        public RelQueryBuilder ToBuilder()
        {
            return this;
        }

        public override string ToString()
        {
            return _sb.ToString();
        }

        public RelQueryBuilder Print(Action<string> write)
        {
            write(ToString());
            return this;
        }
    }
}