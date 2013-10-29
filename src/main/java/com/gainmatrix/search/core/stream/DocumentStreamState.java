package com.gainmatrix.search.core.stream;

/**
 * Описание текущего документа в потоке - документа на который была установлена текущая позиция потока
 * методами next() или seek()
 * @see DocumentStream#next()
 * @see DocumentStream#seek(long)
 * @param <M> Meta description class
 */
public interface DocumentStreamState<M> {

    /**
     * Constant which describes missing document
     */
    long NO_DOCUMENT = Long.MAX_VALUE;

    /**
     * Запрос идентификатора текущего документа. Возвращает то же самое значение, что вернул последний вызов методов
     * next() или seek() - если он был успешным. Вызов метода допустим только в случае когда вызов next() или seek()
     * вернул значение отличное от NO_DOCUMENT
     * @return Идентификатор текущего документа
     * @see DocumentStreamNavigation#next()
     * @see DocumentStreamNavigation#seek(long)
     * @see DocumentStreamState#NO_DOCUMENT
     */
    long getId();

    /**
     * This method allows to inspect the whole stream hierarchy which allows to know which stream leads to the current
     * document. Must be called only when next() or seek(long) returned valid document identifier.
     * @param visitor Stream hierarchy visitor instance
     * @see DocumentStreamNavigation#next()
     * @see DocumentStreamNavigation#seek(long)
     */
    void visit(DocumentStreamVisitor<M> visitor);

}
