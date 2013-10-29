package com.gainmatrix.search.core.stream;

import java.io.Closeable;

/**
 * Описание абстрактного потока документов которые представлены уникальным идентификатором и весом. Идентификаторы в
 * диапазоне [0..Long.MAX_VALUE-1] в потоке уникальны (нет повторений) и поток возвращает их строго в порядке
 * возрастания их значения (они отсортированы).
 * @param <M> Meta description class
 */
public interface DocumentStream<M> extends DocumentStreamState<M>, DocumentStreamNavigation, Closeable {

    /**
     * Открывает поток, выделяет требуемые ресурсы и возвращает объект-описание потока. После вызова этой функции поток
     * готов к навигации методами next() и seek(), а после использования должен быть закрыт методом close(). Некоторые
     * реализации потоков допускают повторное переоткрытие потока после его закрытия.
     * @return Описание потока
     */
    DocumentStreamDescription open();

    /**
     * Request stream meta information
     * @return Meta description
     */
    M getMeta();

    /**
     * Закрывает поток и освобождает занятые им ресурсы. Метод может быть вызван в любой момент и должен допускать
     * повторные вызовы, в том числе и вызовы осуществляемые без предварительного вызова метода open()
     */
    @Override
    void close();

}
