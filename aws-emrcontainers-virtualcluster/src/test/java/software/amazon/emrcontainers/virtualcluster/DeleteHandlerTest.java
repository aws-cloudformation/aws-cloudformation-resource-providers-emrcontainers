package software.amazon.emrcontainers.virtualcluster;

import com.amazonaws.services.emrcontainers.model.*;
import org.mockito.ArgumentMatchers;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private Logger logger;

    private final String virtualClusterId = "virtualClusterId";
    private DeleteHandler handler;
    private ResourceModel model;
    private ResourceHandlerRequest<ResourceModel> request;

    @BeforeEach
    public void setup() {
        proxy = mock(AmazonWebServicesClientProxy.class);
        logger = mock(Logger.class);
        handler = new DeleteHandler();
        model = ResourceModel.builder()
                .id(virtualClusterId)
                .build();
        request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();
    }

    @Test
    public void handleRequest_deleteInitiated() {
        DeleteVirtualClusterResult deleteVirtualClusterResult = new DeleteVirtualClusterResult();
        doReturn(deleteVirtualClusterResult)
                .when(proxy)
                .injectCredentialsAndInvoke(any(DeleteVirtualClusterRequest.class), ArgumentMatchers.<Function<DeleteVirtualClusterRequest, DeleteVirtualClusterResult>>any());

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext().getIsDeleteInProgress()).isTrue();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(Constants.CALLBACK_DELAY_SECONDS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        assertThat(response.getResourceModel().getId()).isEqualTo(virtualClusterId);
    }

    @Test
    public void handleRequest_TerminationStabilizing() {
        VirtualCluster virtualCluster = new VirtualCluster();
        virtualCluster.setState("TERMINATING");

        DescribeVirtualClusterResult describeVirtualClusterResult = new DescribeVirtualClusterResult();
        describeVirtualClusterResult.setVirtualCluster(virtualCluster);
        doReturn(describeVirtualClusterResult)
                .when(proxy)
                .injectCredentialsAndInvoke(any(DescribeVirtualClusterRequest.class), ArgumentMatchers.<Function<DescribeVirtualClusterRequest, DescribeVirtualClusterResult>>any());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
                proxy, request, CallbackContext.builder().isDeleteInProgress(true).build(), logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext().getIsDeleteInProgress()).isTrue();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(Constants.CALLBACK_DELAY_SECONDS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        assertThat(response.getResourceModel().getId()).isEqualTo(virtualClusterId);
    }

    @Test
    public void handleRequest_TerminationStabilized() {
        VirtualCluster virtualCluster = new VirtualCluster();
        virtualCluster.setState("TERMINATED");

        DescribeVirtualClusterResult describeVirtualClusterResult = new DescribeVirtualClusterResult();
        describeVirtualClusterResult.setVirtualCluster(virtualCluster);
        doReturn(describeVirtualClusterResult)
                .when(proxy)
                .injectCredentialsAndInvoke(any(DescribeVirtualClusterRequest.class), ArgumentMatchers.<Function<DescribeVirtualClusterRequest, DescribeVirtualClusterResult>>any());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
                proxy, request, CallbackContext.builder().isDeleteInProgress(true).build(), logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        assertThat(response.getResourceModel()).isNull();
    }

    @Test
    public void handleRequest_ResourceNotFound_TerminationSuceeded() {
        doThrow(ResourceNotFoundException.class)
                .when(proxy)
                .injectCredentialsAndInvoke(any(DeleteVirtualClusterRequest.class), ArgumentMatchers.<Function<DeleteVirtualClusterRequest, DeleteVirtualClusterResult>>any());

        assertThrows(CfnNotFoundException.class, () -> {
            handler.handleRequest(proxy, request, null, logger);
        });
    }

    @Test
    public void handleRequest_ClusterTerminated_ThrowsCfnNotFoundException() {
        ValidationException validationException = new ValidationException("vcid " + Constants.VIRTUAL_CLUSTER_TERMINATED_MESSAGE);
        doThrow(validationException)
                .when(proxy)
                .injectCredentialsAndInvoke(any(DeleteVirtualClusterRequest.class), ArgumentMatchers.<Function<DeleteVirtualClusterRequest, DeleteVirtualClusterResult>>any());

        assertThrows(CfnNotFoundException.class, () -> {
            handler.handleRequest(proxy, request, null, logger);
        });
    }

    @Test
    public void handleRequest_Arrested_TerminationFailed() {
        VirtualCluster virtualCluster = new VirtualCluster();
        virtualCluster.setState("ARRESTED");

        DescribeVirtualClusterResult describeVirtualClusterResult = new DescribeVirtualClusterResult();
        describeVirtualClusterResult.setVirtualCluster(virtualCluster);
        doReturn(describeVirtualClusterResult)
                .when(proxy)
                .injectCredentialsAndInvoke(any(DescribeVirtualClusterRequest.class), ArgumentMatchers.<Function<DescribeVirtualClusterRequest, DescribeVirtualClusterResult>>any());

        CallbackContext callbackContext = CallbackContext.builder().isDeleteInProgress(true).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(
                proxy, request, callbackContext, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isEqualTo(callbackContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isEqualTo("Cluster virtualClusterId is in arrested state and failed to stabilize");
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotStabilized);

        assertThat(response.getResourceModel().getId()).isEqualTo(virtualClusterId);
    }
}
