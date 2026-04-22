using Soenneker.ServiceBus.Transmitter.Abstract;
using Soenneker.Tests.HostedUnit;

namespace Soenneker.ServiceBus.Transmitter.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public class ServiceBusTransmitterTests : HostedUnitTest
{
    private readonly IServiceBusTransmitter _util;

    public ServiceBusTransmitterTests(Host host) : base(host)
    {
        _util = Resolve<IServiceBusTransmitter>(true);
    }

    [Test]
    public void Default()
    {

    }
}
