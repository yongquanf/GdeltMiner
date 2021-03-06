/*
 * Copyright 2008 Jonathan Ledlie and Peter Pietzuch
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 *
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package EventGraph.IncrementalLearning.NC;

/**
 * An observer to be notified when the application coordinates change.
 * 
 * @author Michael Parker, Jonathan Ledlie
 */
public interface ApplicationObserver {
	/**
	 * This method is invoked when the application-level coordinates are
	 * updated.
	 * 
	 * @param new_coords
	 *            the new application-level coordinates
	 */
	public void coordinatesUpdated(Coordinate new_coords);
}
