using Soenneker.ServiceBus.Transmitter.Abstract;
using Soenneker.Tests.FixturedUnit;
using Xunit;

namespace Soenneker.ServiceBus.Transmitter.Tests;

[Collection("Collection")]
public class ServiceBusTransmitterTests : FixturedUnitTest
{
    private readonly IServiceBusTransmitter _util;

    public ServiceBusTransmitterTests(Fixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
        _util = Resolve<IServiceBusTransmitter>(true);
    }

    [Fact]
    public void Default()
    {

    }
}
