package analytics.util.objects;

public class TagVariable {
private String tag;
private String modelId;
private String variable;
public TagVariable(String tag, String modelId, String variable){
	this.tag = tag;
	this.modelId = modelId;
	this.variable = variable;
}
public String getTag() {
	return tag;
}
public void setTag(String tag) {
	this.tag = tag;
}
public String getModelId() {
	return modelId;
}
public void setModelId(String modelId) {
	this.modelId = modelId;
}
public String getVariable() {
	return variable;
}
public void setVariable(String variable) {
	this.variable = variable;
}

}
