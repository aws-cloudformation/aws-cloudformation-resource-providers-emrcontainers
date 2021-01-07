package software.amazon.emrcontainers.virtualcluster;

import com.amazonaws.services.emrcontainers.model.CreateVirtualClusterResult;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest {

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
        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder()
                .name("virtualClusterName")
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

        CreateVirtualClusterResult createVirtualClusterResult = new CreateVirtualClusterResult();
        createVirtualClusterResult.setId("virtualClusterId");
        createVirtualClusterResult.setName("name");
        createVirtualClusterResult.setArn("arn");
        doReturn(createVirtualClusterResult)
                .when(proxy)
                .injectCredentialsAndInvoke(any(), any());

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        assertThat(response.getResourceModel().getId()).isEqualTo(createVirtualClusterResult.getId());
        assertThat(response.getResourceModel().getName()).isEqualTo(createVirtualClusterResult.getName());
        assertThat(response.getResourceModel().getArn()).isEqualTo(createVirtualClusterResult.getArn());
    }
}
