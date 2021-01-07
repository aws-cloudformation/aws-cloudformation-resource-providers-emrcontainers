# AWS::EMRContainers::VirtualCluster ContainerProvider

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "<a href="#type" title="Type">Type</a>" : <i>String</i>,
    "<a href="#id" title="Id">Id</a>" : <i>String</i>,
    "<a href="#info" title="Info">Info</a>" : <i><a href="containerinfo.md">ContainerInfo</a></i>
}
</pre>

### YAML

<pre>
<a href="#type" title="Type">Type</a>: <i>String</i>
<a href="#id" title="Id">Id</a>: <i>String</i>
<a href="#info" title="Info">Info</a>: <i><a href="containerinfo.md">ContainerInfo</a></i>
</pre>

## Properties

#### Type

The type of the container provider

_Required_: Yes

_Type_: String

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Id

The ID of the container cluster

_Required_: Yes

_Type_: String

_Minimum_: <code>1</code>

_Maximum_: <code>100</code>

_Pattern_: <code>^[0-9A-Za-z][A-Za-z0-9\-_]*</code>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Info

_Required_: Yes

_Type_: <a href="containerinfo.md">ContainerInfo</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)
