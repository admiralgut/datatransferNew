/**
 * Copyright 2014, 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.run.exceptions;

/**
 * Defines the location of a parsing error
 */
public abstract class ParsingError
{
    private final int lineNo;

    public ParsingError(int lineNo)
    {
        this.lineNo = lineNo;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ParsingError other = (ParsingError) obj;
        if (lineNo != other.lineNo)
            return false;
        return true;
    }

    public int getLineNo()
    {
        return lineNo;
    }

    public abstract String getMessage();

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + lineNo;
        return result;
    }
}