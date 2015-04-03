package com.theladders.storm.exception;

public interface ExceptionClassifier
{
  boolean isSatisfiedBy(Throwable t);
}
