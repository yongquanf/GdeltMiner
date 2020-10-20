package util.web;

/**
 * @author Kevin Chen
 */
public class GdeltException extends RuntimeException
{
    public GdeltException(String message)
    {
        super(message);
    }

    public GdeltException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
