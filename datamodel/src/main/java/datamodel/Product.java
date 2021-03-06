/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package datamodel;

import java.io.Serializable;

public class Product implements Serializable {

    private final int id;
    private final String name;

    public Product(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        Product that;
        return obj instanceof Product
                && this.id == (that = (Product) obj).id
                && this.name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int hc = 17;
        hc = 73 * hc + id;
        hc = 73 * hc + name.hashCode();
        return hc;
    }

    @Override
    public String toString() {
        return "Product{name=" + name + ", id=" + id + '}';
    }
}
