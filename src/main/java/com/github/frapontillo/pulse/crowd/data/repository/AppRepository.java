/*
 * Copyright 2015 Francesco Pontillo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.frapontillo.pulse.crowd.data.repository;

import com.github.frapontillo.pulse.crowd.data.entity.App;
import org.bson.types.ObjectId;

/**
 * {@link Repository} for {@link App}s.
 *
 * @author Francesco Pontillo
 */
public class AppRepository extends Repository<App, ObjectId> {
    @Override public String getCollectionName() {
        return "App";
    }

    @Override public Class<App> getMappedClass() {
        return App.class;
    }
}
