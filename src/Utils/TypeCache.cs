using System;

namespace Soenneker.ServiceBus.Transmitter.Utils;

internal static class TypeCache<T>
{
    public static readonly Type Type = typeof(T);
    public static readonly string TypeName = Type.FullName ?? Type.Name;
}