package software.amazon.emrcontainers.virtualcluster;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.emrcontainers.model.*;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
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

import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private Logger logger;

    @Captor
    ArgumentCaptor<AmazonWebServiceRequest> amazonWebServiceRequestArgumentCaptor;

    private final UpdateHandler handler = new UpdateHandler();

    private final String VIRTUAL_CLUSTER_ID = "virtucalclusterId";
    private final String VIRTUAL_CLUSTER_NAME = "virtucalclusterName";
    private final String VIRTUAL_CLUSTER_ARN = "virtualclusterArn";

    @BeforeEach
    public void setup() {
        proxy = mock(AmazonWebServicesClientProxy.class);
        logger = mock(Logger.class);
        amazonWebServiceRequestArgumentCaptor = ArgumentCaptor.forClass(AmazonWebServiceRequest.class);
    }

    @Test
    public void handleRequest_NothingToUpdate_Success() {
        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .desiredResourceTags(null)
                .build();

        doReturn(getDescribeVirtualClusterResult(VirtualClusterState.RUNNING))
                .when(proxy)
                .injectCredentialsAndInvoke(any(), any());

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_VirtualClusterGarbageCollected_ReturnsCfnNotFoundException() {
        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .desiredResourceTags(null)
            .build();

        doThrow(ResourceNotFoundException.class)
            .when(proxy)
            .injectCredentialsAndInvoke(any(), any());

        Assertions.assertThrows(CfnNotFoundException.class, () -> {
            handler.handleRequest(proxy, request, null, logger);
        });
    }

    @Test
    public void handleRequest_VirtualClusterTerminated_ReturnsCfnNotFoundException() {
        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .desiredResourceTags(null)
            .build();

        doReturn(getDescribeVirtualClusterResult(VirtualClusterState.TERMINATED))
            .when(proxy)
            .injectCredentialsAndInvoke(any(DescribeVirtualClusterRequest.class), any());

        Assertions.assertThrows(CfnNotFoundException.class, () -> {
            handler.handleRequest(proxy, request, null, logger);
        });
    }

    @Test
    public void handleRequest_NewTags_TagsAdded() {
        final ResourceModel model = ResourceModel.builder()
                .id(VIRTUAL_CLUSTER_ID)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .desiredResourceTags(ImmutableMap.of("key1", "val1"))
                .build();

        doReturn(getDescribeVirtualClusterResult(VirtualClusterState.RUNNING))
                .when(proxy)
                .injectCredentialsAndInvoke(any(DescribeVirtualClusterRequest.class), any());

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);
        validate(response);

        verify(proxy, times(2))
                .injectCredentialsAndInvoke(amazonWebServiceRequestArgumentCaptor.capture(), any());

        // Verify tagging operations
        List<TagResourceRequest> tagResourceRequestList = getRequestArguments(amazonWebServiceRequestArgumentCaptor, TagResourceRequest.class);
        assertThat(tagResourceRequestList.size()).isEqualTo(1);

        Map<String, String> addedTags =  tagResourceRequestList.get(0).getTags();
        assertThat(addedTags.size()).isEqualTo(1);
        assertThat(addedTags.get("key1")).isEqualTo("val1");

        List<UntagResourceRequest> untagResourceRequestList = getRequestArguments(amazonWebServiceRequestArgumentCaptor, UntagResourceRequest.class);
        assertThat(untagResourceRequestList.size()).isEqualTo(0);
    }

    @Test
    public void handleRequeset_OldTags_TagsRemoved() {
        final ResourceModel model = ResourceModel.builder()
                .id(VIRTUAL_CLUSTER_ID)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        doReturn(getDescribeVirtualClusterResult(VirtualClusterState.RUNNING, ImmutableMap.of("key1", "val1")))
                .when(proxy)
                .injectCredentialsAndInvoke(any(DescribeVirtualClusterRequest.class), any());

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);
        validate(response);

        verify(proxy, times(2))
                .injectCredentialsAndInvoke(amazonWebServiceRequestArgumentCaptor.capture(), any());

        // Verify tagging operations
        List<TagResourceRequest> tagResourceRequestList = getRequestArguments(amazonWebServiceRequestArgumentCaptor, TagResourceRequest.class);
        assertThat(tagResourceRequestList.size()).isEqualTo(0);

        List<UntagResourceRequest> untagResourceRequestList = getRequestArguments(amazonWebServiceRequestArgumentCaptor, UntagResourceRequest.class);
        assertThat(untagResourceRequestList.size()).isEqualTo(1);
        List<String> removedTagKeys =  untagResourceRequestList.get(0).getTagKeys();
        assertThat(removedTagKeys.size()).isEqualTo(1);
        assertThat(removedTagKeys.get(0)).isEqualTo("key1");
    }

    @Test
    public void handleRequest_OldAndNewTags_TagsAddedAndRemoved() {
        final ResourceModel model = ResourceModel.builder()
                .id(VIRTUAL_CLUSTER_ID)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .desiredResourceTags(ImmutableMap.of("key1", "val1", "key4", "val4"))
                .build();

        doReturn(getDescribeVirtualClusterResult(VirtualClusterState.RUNNING, ImmutableMap.of("key1", "val1", "key2", "val2", "key3", "val3")))
                .when(proxy)
                .injectCredentialsAndInvoke(any(DescribeVirtualClusterRequest.class), any());

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        validate(response);
        verify(proxy, times(3))
                .injectCredentialsAndInvoke(amazonWebServiceRequestArgumentCaptor.capture(), any());

        // Verify tagging operations
        List<TagResourceRequest> tagResourceRequestList = getRequestArguments(amazonWebServiceRequestArgumentCaptor, TagResourceRequest.class);
        assertThat(tagResourceRequestList.size()).isEqualTo(1);

        Map<String, String> addedTags =  tagResourceRequestList.get(0).getTags();
        assertThat(addedTags.size()).isEqualTo(1);
        assertThat(addedTags.get("key4")).isEqualTo("val4");

        List<UntagResourceRequest> untagResourceRequestList = getRequestArguments(amazonWebServiceRequestArgumentCaptor, UntagResourceRequest.class);
        assertThat(untagResourceRequestList.size()).isEqualTo(1);
        List<String> removedTagKeys =  untagResourceRequestList.get(0).getTagKeys();
        assertThat(removedTagKeys.size()).isEqualTo(2);
        assertThat(removedTagKeys.contains("key2")).isTrue();
        assertThat(removedTagKeys.contains("key3")).isTrue();
    }

    private DescribeVirtualClusterResult getDescribeVirtualClusterResult(VirtualClusterState virtualClusterState) {
        return getDescribeVirtualClusterResult(virtualClusterState, Collections.emptyMap());
    }

    @SuppressWarnings("unchecked")
    private <T extends AmazonWebServiceRequest> List<T> getRequestArguments(ArgumentCaptor<AmazonWebServiceRequest> argumentCaptor, Class<T> cls) {
        return argumentCaptor.getAllValues().stream()
                .filter(request -> cls.isInstance(request))
                .map(request -> (T) request)
                .collect(Collectors.toList());
    }

    private void validate(ProgressEvent<ResourceModel, CallbackContext> response) {
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        ResourceModel model = response.getResourceModel();
        assertThat(model.getId()).isEqualTo(VIRTUAL_CLUSTER_ID);
        assertThat(model.getName()).isEqualTo(VIRTUAL_CLUSTER_NAME);
        assertThat(model.getArn()).isEqualTo(VIRTUAL_CLUSTER_ARN);
    }

    private DescribeVirtualClusterResult getDescribeVirtualClusterResult(VirtualClusterState virtualClusterState, Map<String, String> tags) {
        VirtualCluster virtualCluster = new VirtualCluster();
        virtualCluster.setId(VIRTUAL_CLUSTER_ID);
        virtualCluster.setName(VIRTUAL_CLUSTER_NAME);
        virtualCluster.setState(virtualClusterState.toString());
        virtualCluster.setArn(VIRTUAL_CLUSTER_ARN);
        virtualCluster.setTags(tags);

        DescribeVirtualClusterResult describeVirtualClusterResult = new DescribeVirtualClusterResult();
        describeVirtualClusterResult.setVirtualCluster(virtualCluster);
        return describeVirtualClusterResult;
    }
}
