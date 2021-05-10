package software.amazon.emrcontainers.virtualcluster;

import com.amazonaws.services.emrcontainers.model.ListVirtualClustersRequest;
import com.amazonaws.services.emrcontainers.model.ListVirtualClustersResult;
import com.amazonaws.services.emrcontainers.model.VirtualCluster;
import com.amazonaws.services.emrcontainers.model.VirtualClusterState;
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

import java.util.Arrays;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest {

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
    public void handleRequest_Success() {
        final ListHandler handler = new ListHandler();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(ResourceModel.builder().build())
                .build();
        ListVirtualClustersResult listVirtualClustersResult = new ListVirtualClustersResult();
        listVirtualClustersResult.setVirtualClusters(Arrays.asList(
            buildVirtualCluster("1", VirtualClusterState.RUNNING), buildVirtualCluster("2", VirtualClusterState.TERMINATED)));
        doReturn(listVirtualClustersResult)
                .when(proxy)
                .injectCredentialsAndInvoke(any(ListVirtualClustersRequest.class), any(Function.class));

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        assertThat(response.getResourceModels().size()).isEqualTo(1);
        ResourceModel model = response.getResourceModels().get(0);
        assertThat(model.getId()).isEqualTo("1");
        assertThat(model.getName()).isEqualTo("name1");
        assertThat(model.getArn()).isEqualTo("arn1");
        assertThat(model.getContainerProvider().getId()).isEqualTo("eksId1");
        assertThat(model.getContainerProvider().getType()).isEqualTo("eks");
        assertThat(model.getContainerProvider().getInfo().getEksInfo().getNamespace()).isEqualTo("namespace1");
    }

    private VirtualCluster buildVirtualCluster(String id, VirtualClusterState virtualClusterState) {
        com.amazonaws.services.emrcontainers.model.EksInfo eksInfo =
                new com.amazonaws.services.emrcontainers.model.EksInfo();
        eksInfo.setNamespace("namespace" + id);

        com.amazonaws.services.emrcontainers.model.ContainerInfo containerInfo =
                new com.amazonaws.services.emrcontainers.model.ContainerInfo();
        containerInfo.setEksInfo(eksInfo);

        com.amazonaws.services.emrcontainers.model.ContainerProvider containerProvider =
                new com.amazonaws.services.emrcontainers.model.ContainerProvider();
        containerProvider.setInfo(containerInfo);
        containerProvider.setId("eksId" + id);
        containerProvider.setType("eks");

        VirtualCluster virtualCluster = new VirtualCluster();
        virtualCluster.setId(id);
        virtualCluster.setState(virtualClusterState.toString());
        virtualCluster.setName("name" + id);
        virtualCluster.setArn("arn" + id);
        virtualCluster.setContainerProvider(containerProvider);
        return virtualCluster;
    }
}
