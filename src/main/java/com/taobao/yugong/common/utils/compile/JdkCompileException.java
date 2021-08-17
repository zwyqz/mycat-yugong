package com.taobao.yugong.common.utils.compile;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;

/**
 * @author agapple 2014年2月25日 下午11:38:06
 * @since 1.0.0
 */
public class JdkCompileException extends Exception {

  private static final long serialVersionUID = 1L;

  private transient DiagnosticCollector<JavaFileObject> diagnostics;

  public JdkCompileException(String message, Throwable cause, DiagnosticCollector<JavaFileObject> diagnostics) {
    super(message, cause);
    setDiagnostics(diagnostics);
  }

  public JdkCompileException(String message, DiagnosticCollector<JavaFileObject> diagnostics) {
    super(message);
    setDiagnostics(diagnostics);
  }

  public JdkCompileException(Throwable cause, DiagnosticCollector<JavaFileObject> diagnostics) {
    super(cause);
    setDiagnostics(diagnostics);
  }

  private void setDiagnostics(DiagnosticCollector<JavaFileObject> diagnostics) {
    this.diagnostics = diagnostics;
  }

  public DiagnosticCollector<JavaFileObject> getDiagnostics() {
    return diagnostics;
  }

}
