package com.taobao.yugong.common.utils.compile;

import com.taobao.yugong.exception.YuGongException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

/**
 * @author agapple 2014年2月25日 下午11:38:06
 * @since 1.0.0
 */
public class JdkCompiler implements JavaSourceCompiler {

  private List<String> options = new ArrayList<String>();

  public JdkCompiler() {
  }

  public Class compile(String sourceString) {
    return compile(new JavaSource(sourceString));
  }

  private Class compile(JavaSource javaSource) {
    try {
      JdkCompileTask compileTask = new JdkCompileTask(new JdkCompilerClassLoader(this.getClass().getClassLoader()),
          options);
      return compileTask.compile(javaSource.getPackageName(), javaSource.getClassName(), javaSource.getSource());
    } catch (JdkCompileException ex) {
      DiagnosticCollector<JavaFileObject> diagnostics = ex.getDiagnostics();
      throw new YuGongException("source:" + javaSource + ", " + diagnostics.getDiagnostics(), ex);
    } catch (Throwable ex) {
      throw new YuGongException("source:" + javaSource, ex);
    }

  }

  public static URI toURI(String name) {
    try {
      return new URI(name);
    } catch (URISyntaxException e) {
      throw new YuGongException(e);
    }
  }

  public static class JdkCompileTask<T> {

    public static final String EXTENSION = ".java";
    public static final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    private List<String> options;
    private JdkCompilerClassLoader classLoader;

    public JdkCompileTask(JdkCompilerClassLoader classLoader, List<String> options) {
      if (compiler == null) {
        throw new YuGongException("Can't find java compiler , pls check tools.jar");
      }
      this.classLoader = classLoader;
      this.options = options;
    }

    public synchronized Class compile(String packageName, String className, final CharSequence javaSource)
        throws JdkCompileException,
        ClassCastException {
      DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
      JavaFileManagerImpl javaFileManager = buildFileManager(classLoader, classLoader.getParent(), diagnostics);

      JavaFileObjectImpl source = new JavaFileObjectImpl(className, javaSource);
      javaFileManager.putFileForInput(StandardLocation.SOURCE_PATH, packageName, className + EXTENSION, source);

      CompilationTask task = compiler.getTask(null,
          javaFileManager,
          diagnostics,
          options,
          null,
          Arrays.asList(source));
      final Boolean result = task.call();
      if (result == null || !result.booleanValue()) {
        throw new JdkCompileException("Compilation failed.", diagnostics);
      }

      try {
        return (Class<T>) classLoader.loadClass(packageName + "." + className);
      } catch (Throwable e) {
        throw new JdkCompileException(e, diagnostics);
      }
    }

    private JavaFileManagerImpl buildFileManager(JdkCompilerClassLoader classLoader, ClassLoader loader,
        DiagnosticCollector<JavaFileObject> diagnostics) {
      StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null);
      if (loader instanceof URLClassLoader
          && (!"sun.misc.Launcher$AppClassLoader".equalsIgnoreCase(loader.getClass().getName()))) {
        try {
          URLClassLoader urlClassLoader = (URLClassLoader) loader;
          List<File> paths = new ArrayList<File>();
          for (URL url : urlClassLoader.getURLs()) {
            File file = new File(url.getFile());
            paths.add(file);
          }

          fileManager.setLocation(StandardLocation.CLASS_PATH, paths);
        } catch (Throwable e) {
          throw new YuGongException(e);
        }
      }

      return new JavaFileManagerImpl(fileManager, classLoader);
    }

  }

  public static class JavaFileManagerImpl extends ForwardingJavaFileManager<JavaFileManager> {

    private final JdkCompilerClassLoader classLoader;
    private final Map<URI, JavaFileObject> fileDatas = new HashMap<URI, JavaFileObject>();

    public JavaFileManagerImpl(JavaFileManager fileManager, JdkCompilerClassLoader classLoader) {
      super(fileManager);
      this.classLoader = classLoader;
    }

    public void putFileForInput(StandardLocation location, String packageName, String relativeName,
        JavaFileObject file) {
      fileDatas.put(clasURI(location, packageName, relativeName), file);
    }

    @Override
    public FileObject getFileForInput(Location location, String packageName, String relativeName)
        throws IOException {
      FileObject o = fileDatas.get(clasURI(location, packageName, relativeName));
      if (o == null) {
        return super.getFileForInput(location, packageName, relativeName);
      } else {
        return o;
      }
    }

    @Override
    public String inferBinaryName(Location loc, JavaFileObject file) {
      // 自定义实现
      if (file instanceof JavaFileObjectImpl) {
        return file.getName();
      } else {
        return super.inferBinaryName(loc, file);
      }
    }

    @Override
    public JavaFileObject getJavaFileForOutput(Location location, String qualifiedName, Kind kind,
        FileObject outputFile) throws IOException {
      JavaFileObject file = new JavaFileObjectImpl(qualifiedName, kind);
      classLoader.add(qualifiedName, file);
      return file;
    }

    @Override
    public ClassLoader getClassLoader(JavaFileManager.Location location) {
      return classLoader;
    }

    @Override
    public Iterable<JavaFileObject> list(Location location, String packageName, Set<Kind> kinds, boolean recurse)
        throws IOException {
      Iterable<JavaFileObject> files = super.list(location, packageName, kinds, recurse);
      List<JavaFileObject> result = new ArrayList<JavaFileObject>();
      for (JavaFileObject file : files) {
        result.add(file);
      }
      if (StandardLocation.CLASS_PATH == location && kinds.contains(JavaFileObject.Kind.CLASS)) {
        for (JavaFileObject file : fileDatas.values()) {
          if (Kind.CLASS == file.getKind() && file.getName().startsWith(packageName)) {
            result.add(file);
          }
        }

        result.addAll(classLoader.getAllFiles());
      }

      if (StandardLocation.SOURCE_PATH == location && kinds.contains(JavaFileObject.Kind.SOURCE)) {
        for (JavaFileObject file : fileDatas.values()) {
          if (Kind.SOURCE == file.getKind() && file.getName().startsWith(packageName)) {
            result.add(file);
          }
        }
      }

      return result;
    }

    private URI clasURI(Location location, String packageName, String relativeName) {
      return toURI(location.getName() + '/' + packageName + '/' + relativeName);
    }
  }

  public static class JavaFileObjectImpl extends SimpleJavaFileObject {

    private ByteArrayOutputStream byteCode = new ByteArrayOutputStream();
    private CharSequence source;

    public JavaFileObjectImpl(final String baseName, final CharSequence source) {
      super(toURI(baseName + JdkCompileTask.EXTENSION), Kind.SOURCE);
      this.source = source;
    }

    public JavaFileObjectImpl(final String name, final Kind kind) {
      super(toURI(name), kind);
      source = null;
    }

    public JavaFileObjectImpl(URI uri, Kind kind) {
      super(uri, kind);
      source = null;
    }

    @Override
    public CharSequence getCharContent(final boolean ignoreEncodingErrors) throws UnsupportedOperationException {
      if (source == null) {
        throw new UnsupportedOperationException();
      } else {
        return source;
      }
    }

    @Override
    public InputStream openInputStream() {
      return new ByteArrayInputStream(byteCode.toByteArray());
    }

    @Override
    public OutputStream openOutputStream() {
      return byteCode;
    }

    public byte[] getBytes() {
      return byteCode.toByteArray();
    }
  }

  public final class JdkCompilerClassLoader extends ClassLoader {

    private final Map<String, JavaFileObject> classes = new HashMap<String, JavaFileObject>();

    public JdkCompilerClassLoader(ClassLoader parentClassLoader) {
      super(parentClassLoader);
    }

    public Collection<JavaFileObject> getAllFiles() {
      return classes.values();
    }

    protected synchronized Class<?> findClass(String qualifiedClassName) throws ClassNotFoundException {
      JavaFileObject file = classes.get(qualifiedClassName);
      if (file != null) {
        byte[] bytes = ((JavaFileObjectImpl) file).getBytes();
        return defineClass(qualifiedClassName, bytes, 0, bytes.length);
      }

      try {
        return Class.forName(qualifiedClassName);
      } catch (ClassNotFoundException nf) {
        // Ignore
      }

      try {
        return Thread.currentThread().getContextClassLoader().loadClass(qualifiedClassName);
      } catch (ClassNotFoundException nf) {
        // Ignore
      }

      return super.findClass(qualifiedClassName);
    }

    public void add(String qualifiedClassName, final JavaFileObject javaFile) {
      classes.put(qualifiedClassName, javaFile);
    }

    protected synchronized Class<?> loadClass(final String name, final boolean resolve)
        throws ClassNotFoundException {
      try {
        Class c = findClass(name);
        if (c != null) {
          if (resolve) {
            resolveClass(c);
          }

          return c;
        }
      } catch (ClassNotFoundException e) {
        // Ignore and fall through
      }

      return super.loadClass(name, resolve);
    }

    public InputStream getResourceAsStream(final String name) {
      if (name.endsWith(".class")) {
        String qualifiedClassName = name.substring(0, name.length() - ".class".length()).replace('/', '.');
        JavaFileObjectImpl file = (JavaFileObjectImpl) classes.get(qualifiedClassName);

        if (file != null) {
          return new ByteArrayInputStream(file.getBytes());
        }
      }

      return super.getResourceAsStream(name);
    }

    public void clearCache() {
      this.classes.clear();
    }

    public JavaFileObject getJavaFileObject(String qualifiedClassName) {
      return classes.get(qualifiedClassName);
    }
  }

}
