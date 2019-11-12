package anatoliisatanovskyi.bigdata201.pipelining.exception;

public class CoordsProcessorException extends Exception {

	private static final long serialVersionUID = 1L;

	public CoordsProcessorException(String message) {
		this(message, null);
	}

	public CoordsProcessorException(String message, Exception cause) {
		super(message, cause);
	}
}
