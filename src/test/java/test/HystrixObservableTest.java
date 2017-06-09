package test;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixThreadPoolKey;

import rx.Single;
import rx.subjects.ReplaySubject;

public class HystrixObservableTest {

	public abstract class TestCommand extends HystrixCommand<String> {

		private final String name;
		
		protected TestCommand(String name) {
			super(HystrixCommand.Setter.withGroupKey(
					HystrixCommandGroupKey.Factory.asKey(name))
	                	.andCommandKey(HystrixCommandKey.Factory.asKey(name))
	                	.andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey(name)));
			
			this.name = name;
		}
		
		@Override
		protected String run() throws Exception {
			return name;
		}
	}
	
	public class FooCommand extends TestCommand {
		
		protected FooCommand() {
			super("Foo");
		}
		
		@Override
		protected String run() throws Exception {
			fooExecutionCounter++;
			return super.run();
		}
	}
	
	public class BarCommand extends TestCommand {
		
		private final String arg;
		
		protected BarCommand(String arg) {
			super("Bar");
			this.arg = arg;
		}
		
		@Override
		protected String run() throws Exception {
			barExecutionCounter++;
			return String.format("Bar[%s]", arg);
		}
	}
	
	public class BazCommand extends TestCommand {
		
		protected BazCommand() {
			super("Baz");
		}
		
		@Override
		protected String run() throws Exception {
			bazExecutionCounter++;
			return super.run();
		}
	}
	
	public class QuxCommand extends TestCommand {
		
		protected QuxCommand() {
			super("Qux");
		}
		
		@Override
		protected String run() throws Exception {
			quxExecutionCounter++;
			return super.run();
		}
	}
	
	private static <T> Single<T> toReplayable(Single<T> single) {
		ReplaySubject<T> subject = ReplaySubject.create();
		single.subscribe(subject);
		return subject.toSingle();
	}
	
	private static <T> Single<T> toSingle(HystrixCommand<T> command) {
		return command.observe().toSingle();
	}
	
	private int barExecutionCounter, fooExecutionCounter, bazExecutionCounter, quxExecutionCounter = 0;
	
	@Test
	public void testFoo() {
		Single<String> single = toReplayable(toSingle(new FooCommand()).flatMap(
				fooResult -> {
					// New command creation
					return toSingle(new BarCommand(fooResult));
				}));
		
		Single<String> baz = toSingle(new BazCommand())
				.zipWith(single, (first, second) -> first+second);
		Single<String> qux = toSingle(new QuxCommand())
				.zipWith(single, (first, second) -> first+second);
		
		assertEquals("BazBar[Foo]QuxBar[Foo]", Single.zip(baz, qux, 
				(first, second) -> first+second).toBlocking().value());
		
		assertEquals(1, quxExecutionCounter);
		assertEquals(1, fooExecutionCounter);
		assertEquals(1, bazExecutionCounter);
		assertEquals("Bar command executed multiple times", 1, barExecutionCounter); // XXX Failed assertion
	}
}
