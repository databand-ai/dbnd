/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ErrorInfo {

    private String msg;
    private String helpMsg;
    private Boolean databandError;
    private String traceback;
    private String nested;
    private String userCodeTraceback;
    private Boolean showExcInfo;
    private String excType;

    public ErrorInfo() {
    }

    public ErrorInfo(String msg,
                     String helpMsg,
                     Boolean databandError,
                     String traceback,
                     String nested,
                     String userCodeTraceback,
                     Boolean showExcInfo,
                     String excType) {
        this.msg = msg;
        this.helpMsg = helpMsg;
        this.databandError = databandError;
        this.traceback = traceback;
        this.nested = nested;
        this.userCodeTraceback = userCodeTraceback;
        this.showExcInfo = showExcInfo;
        this.excType = excType;
    }

    public String getMsg() {
        return msg;
    }

    public String getHelpMsg() {
        return helpMsg;
    }

    public Boolean getDatabandError() {
        return databandError;
    }

    public String getTraceback() {
        return traceback;
    }

    public String getNested() {
        return nested;
    }

    public String getUserCodeTraceback() {
        return userCodeTraceback;
    }

    public Boolean getShowExcInfo() {
        return showExcInfo;
    }

    public Object getExcType() {
        return excType;
    }
}
