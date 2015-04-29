package analytics.util.objects;

public class TagVariable {
private String tag;
private int modelId;
private String variable;
TagVariable(String tag, int modelId, String variable){
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
public int getModelId() {
	return modelId;
}
public void setModelId(int modelId) {
	this.modelId = modelId;
}
public String getVariable() {
	return variable;
}
public void setVariable(String variable) {
	this.variable = variable;
}

}
