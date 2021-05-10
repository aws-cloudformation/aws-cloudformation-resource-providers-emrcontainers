package software.amazon.emrcontainers.virtualcluster;

import com.amazonaws.services.emrcontainers.model.DescribeVirtualClusterRequest;
import com.amazonaws.services.emrcontainers.model.DescribeVirtualClusterResult;
import com.amazonaws.services.emrcontainers.model.VirtualCluster;
import com.amazonaws.services.emrcontainers.model.VirtualClusterState;
import org.junit.jupiter.api.Assertions;
import org.mockito.ArgumentMatchers;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private Logger logger;

    @BeforeEach
    public void setup() {
        proxy = mock(AmazonWebServicesClientProxy.class);
        logger = mock(Logger.class);
    }

    @Test
    public void handleRequest_success() {
        final ReadHandler handler = new ReadHandler();

        final String virtualClusterName = "virtualClusterName";
        final ResourceModel model = ResourceModel.builder()
                .name(virtualClusterName)
                .containerProvider(ContainerProvider.builder()
                        .id("eksClusterId")
                        .type("eks")
                        .info(ContainerInfo.builder().eksInfo(
                                EksInfo.builder()
                                        .namespace("namespace")
                                        .build())
                                .build())
                        .build())
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        VirtualCluster virtualCluster = new VirtualCluster();
        virtualCluster.setName(virtualClusterName);
        virtualCluster.setId("virtualClusterId");
        virtualCluster.setState("RUNNING");
        virtualCluster.setContainerProvider(Translator.translate(model.getContainerProvider()));
        DescribeVirtualClusterResult describeVirtualClusterResult = new DescribeVirtualClusterResult();
        describeVirtualClusterResult.setVirtualCluster(virtualCluster);
        doReturn(describeVirtualClusterResult)
                .when(proxy)
                .injectCredentialsAndInvoke(any(DescribeVirtualClusterRequest.class), ArgumentMatchers.<Function<DescribeVirtualClusterRequest, DescribeVirtualClusterResult>>any());
        ;

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        assertThat(response.getResourceModel().getId()).isEqualTo(virtualCluster.getId());
        assertThat(response.getResourceModel().getName()).isEqualTo(virtualCluster.getName());
        assertThat(response.getResourceModel().getContainerProvider()).isNotNull();
        assertThat(response.getResourceModel().getArn()).isEqualTo(virtualCluster.getArn());
    }

    @Test
    public void handleRequest_VirtualClusterTerminated_ThrowsResourceNotFoundException() {
        final ReadHandler handler = new ReadHandler();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(ResourceModel.builder().id("virtualclusterId").build())
            .build();

        VirtualCluster virtualCluster = new VirtualCluster();
        virtualCluster.setState(VirtualClusterState.TERMINATED.toString());
        DescribeVirtualClusterResult describeVirtualClusterResult = new DescribeVirtualClusterResult();
        describeVirtualClusterResult.setVirtualCluster(virtualCluster);
        doReturn(describeVirtualClusterResult)
            .when(proxy)
            .injectCredentialsAndInvoke(any(DescribeVirtualClusterRequest.class), ArgumentMatchers.<Function<DescribeVirtualClusterRequest, DescribeVirtualClusterResult>>any());

        Assertions.assertThrows(CfnNotFoundException.class,
            () -> handler.handleRequest(proxy, request, null, logger));
    }

    @Test
    public void handleRequest_VirtualClusterGarbageCollected_ThrowsResourceNotFoundException() {
        final ReadHandler handler = new ReadHandler();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(ResourceModel.builder().build())
                .build();

        doThrow(CfnNotFoundException.class)
                .when(proxy)
                .injectCredentialsAndInvoke(any(DescribeVirtualClusterRequest.class), ArgumentMatchers.<Function<DescribeVirtualClusterRequest, DescribeVirtualClusterResult>>any());

        Assertions.assertThrows(CfnNotFoundException.class,
                () -> handler.handleRequest(proxy, request, null, logger));
    }
}
